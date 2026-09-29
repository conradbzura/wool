from __future__ import annotations

import asyncio
import gc
import itertools
import uuid
import weakref

import cloudpickle
import pytest

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.pool import SubscriberMeta
from wool.runtime.discovery.pool import _SharedSubscription
from wool.runtime.discovery.pool import _SubscriberKey
from wool.runtime.discovery.pool import install_subscriber_pool
from wool.runtime.worker.metadata import WorkerMetadata


class _StubSubscriber(
    metaclass=SubscriberMeta,
    key=lambda cls, key_value: (cls, key_value),
):
    """Minimal subscriber stub for testing SubscriberMeta."""

    def __init__(self, key_value: str) -> None:
        self.key_value = key_value

    def __reduce__(self):
        return type(self), (self.key_value,)

    async def _shutdown(self) -> None:
        pass

    def __aiter__(self):
        return self._event_stream()

    async def _event_stream(self):
        yield  # pragma: no cover


def _make_event(event_type="worker-added", *, address="127.0.0.1:50051"):
    return DiscoveryEvent(
        event_type,
        metadata=WorkerMetadata(
            uid=uuid.uuid4(),
            address=address,
            pid=1,
            version="1.0.0",
        ),
    )


def _setup_pool():
    """Ensure the subscriber pool exists for direct-construction tests."""
    return install_subscriber_pool()


def _make_shared(source, key="test-key"):
    """Create a _SharedSubscription backed by a raw source via the pool."""
    _setup_pool()
    return _SharedSubscription(
        key=_SubscriberKey(key, lambda: source),
        reduce_info=(type(source), (), {}),
    )


def _numbered_subscriber_class():
    """Build a subscriber class whose instances number themselves.

    Every instance the pool's factory builds takes the next number and
    addresses every event it yields with it, so a consumer can tell
    which instance served it from the events alone. The source never
    ends, so an iterator parked on it stays parked until something
    finalizes the subscriber behind it.
    """
    numbers = itertools.count()

    class _Numbered(
        metaclass=SubscriberMeta,
        key=lambda cls, tag: (cls, tag),
    ):
        def __init__(self, tag: str) -> None:
            self.tag = tag
            self.number = next(numbers)

        async def _shutdown(self) -> None:
            pass

        def __aiter__(self):
            return self._gen()

        async def _gen(self):
            while True:
                yield _make_event(address=f"127.0.0.1:{50051 + self.number}")

    return _Numbered


@pytest.mark.asyncio
async def test_install_subscriber_pool_should_return_the_pool_subscribers_cache_in(
    subscriber_pool,
):
    """Test the installed pool is the one a subscriber caches itself in.

    Given:
        A context carrying the subscriber pool the autouse fixture
        installed.
    When:
        A subscription built in that context is iterated as far as its
        first event, so the pool has built the subscriber behind it and
        still holds a reference on it.
    Then:
        It should hold that subscriber in the installed pool, which is
        what makes clearing that object a teardown of what the test
        actually built rather than of an empty pool nothing ever used.
    """

    # Arrange
    shared = _numbered_subscriber_class()("installed-pool-key")
    events = aiter(shared)
    await anext(events)

    # Act
    installed = install_subscriber_pool()

    # Assert
    assert installed is subscriber_pool
    assert subscriber_pool.stats.referenced_entries == 1
    await events.aclose()


@pytest.mark.asyncio
async def test_install_subscriber_pool_should_refuse_a_key_that_cannot_build(
    subscriber_pool,
):
    """Test the pool rejects a key carrying no way to build a subscriber.

    Given:
        The installed subscriber pool, whose every acquisition is made
        under a key that carries its own factory.
    When:
        A resource is acquired under a bare key that carries none.
    Then:
        It should refuse the acquisition, naming the key, rather than
        failing later on an attribute the key never had.
    """
    # Act & assert
    with pytest.raises(TypeError, match="carries no factory"):
        async with subscriber_pool.get("bare-key"):
            pass  # pragma: no cover


class TestSubscriberKey:
    """Tests for _SubscriberKey dataclass.

    Fully qualified name: wool.runtime.discovery.pool._SubscriberKey
    """

    def test___repr___with_identity_delegation(self):
        """Test a key reads as the identity it shares.

        Given:
            Two keys sharing an identity, each carrying a factory of its
            own.
        When:
            Each is rendered the way the pool renders a key in its log
            records.
        Then:
            It should read as the identity alone, so a record names the
            subscriber it concerns rather than a function address that
            differs between two keys the pool treats as one.
        """
        # Arrange
        identity = (_StubSubscriber, "repr-key")
        first = _SubscriberKey(identity, lambda: None)
        second = _SubscriberKey(identity, lambda: None)

        # Act
        rendered = f"{first!r}"

        # Assert
        assert rendered == repr(identity)
        assert rendered == f"{second!r}"


class TestSubscriberMeta:
    """Tests for SubscriberMeta metaclass.

    Fully qualified name: wool.runtime.discovery.pool.SubscriberMeta
    """

    def test___new___with_shared_subscription_wrapping(self):
        """Test SubscriberMeta returns a _SharedSubscription.

        Given:
            A subscriber class using SubscriberMeta.
        When:
            The class is instantiated.
        Then:
            It should return a _SharedSubscription wrapping the
            cached raw subscriber.
        """
        # Act
        result = _StubSubscriber("wrap-key")

        # Assert
        assert isinstance(result, _SharedSubscription)

    @pytest.mark.asyncio
    async def test___new___with_shared_fan_out(self):
        """Test SubscriberMeta shares events across subscriptions.

        Given:
            A source that yields events and two subscriptions
            created with the same key.
        When:
            Both consumers are initialised and one pulls a new
            event.
        Then:
            The other should receive the same event via fan-out.
        """
        # Arrange
        events = [_make_event(address=f"127.0.0.1:{50051 + i}") for i in range(3)]

        class _EventStub(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag):
                self.tag = tag

            async def _shutdown(self):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                for e in events:
                    yield e

        sub_a = _EventStub("shared")
        sub_b = _EventStub("shared")
        it_a = aiter(sub_a)
        it_b = aiter(sub_b)

        # Initialise both consumers — a pulls events[0] (b not
        # registered yet), b gets a replay on its first pull.
        await anext(it_a)
        await anext(it_b)

        # Act — b pulls events[1] from source, fans out to a
        result_b = await anext(it_b)
        result_a = await anext(it_a)

        # Assert — same object via fan-out
        assert result_a is result_b
        assert result_a is events[1]

    @pytest.mark.asyncio
    async def test___new___with_different_keys(self):
        """Test SubscriberMeta isolates different keys.

        Given:
            Two subscriptions created with different keys, each
            backed by a distinct source.
        When:
            Both are iterated.
        Then:
            Events from one key should not appear in the other.
        """
        # Arrange
        event_a = _make_event(address="10.0.0.1:1")
        event_b = _make_event(address="10.0.0.2:2")

        class _IsoStub(
            metaclass=SubscriberMeta,
            key=lambda cls, tag, event: (cls, tag),
        ):
            def __init__(self, tag, event):
                self.tag = tag
                self._event = event

            async def _shutdown(self):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield self._event

        sub_a = _IsoStub("key-a", event_a)
        sub_b = _IsoStub("key-b", event_b)

        # Act
        result_a = await anext(aiter(sub_a))
        result_b = await anext(aiter(sub_b))

        # Assert
        assert result_a is event_a
        assert result_b is event_b

    def test___new___with_lazy_pool_init(self):
        """Test lazy pool initialization when ContextVar is None.

        Given:
            The __subscriber_pool__ ContextVar is explicitly set to
            None.
        When:
            A subscriber is constructed via SubscriberMeta.
        Then:
            The ContextVar should be set to a ResourcePool.
        """
        # Arrange
        __subscriber_pool__.set(None)

        # Act
        _StubSubscriber("lazy-init")

        # Assert
        assert __subscriber_pool__.get() is not None

    def test___new___with_key_callable_receiving_cls(self):
        """Test key callable receives the class as its first argument.

        Given:
            A subscriber class defined with a key callable that
            includes cls in the returned tuple.
        When:
            The class is instantiated.
        Then:
            It should produce a _SharedSubscription whose cache key
            was computed using the class itself.
        """
        # Arrange
        captured = {}

        def capture_key(cls, value):
            captured["cls"] = cls
            captured["value"] = value
            return (cls, value)

        class _CaptureSub(
            metaclass=SubscriberMeta,
            key=capture_key,
        ):
            def __init__(self, value):
                self.value = value

            async def _shutdown(self):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield  # pragma: no cover

        # Act
        result = _CaptureSub("test-val")

        # Assert
        assert isinstance(result, _SharedSubscription)
        assert captured["cls"] is _CaptureSub
        assert captured["value"] == "test-val"

    def test___new___with_inherited_key(self):
        """Test subclass inherits key callable from parent.

        Given:
            A parent subscriber class defined with a key callable
            and a subclass that does not pass its own key.
        When:
            The subclass is instantiated.
        Then:
            It should inherit the parent's key callable and return
            a _SharedSubscription.
        """

        # Arrange
        class _Parent(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag):
                self.tag = tag

            async def _shutdown(self):
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield  # pragma: no cover

        class _Child(_Parent):
            pass

        # Act
        result = _Child("child-tag")

        # Assert
        assert isinstance(result, _SharedSubscription)

    @pytest.mark.asyncio
    async def test___new___should_release_the_class_when_subscriptions_end(
        self, subscriber_pool
    ):
        """Test constructing a subscriber pins its class no longer than it.

        Given:
            A subscriber class defined in this test over a finite
            source, and a subscription over it iterated to exhaustion,
            so the pool has released and finalized the subscriber it
            built and holds nothing under its key.
        When:
            Every reference the test holds is dropped and a collection
            runs.
        Then:
            It should leave nothing referencing the class, so a weak
            reference to it clears — a construction outliving its
            subscriptions pins one class per lifecycle for the life of
            the process.
        """

        # Arrange
        class _Ephemeral(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag: str) -> None:
                self.tag = tag

            async def _shutdown(self) -> None:
                pass

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield _make_event()

        shared = _Ephemeral("ephemeral-key")
        assert [event async for event in shared]
        # Asserted so a lingering entry cannot be mistaken below for the
        # retention this test is about.
        assert subscriber_pool.stats.total_entries == 0
        reference = weakref.ref(_Ephemeral)

        # Act
        del shared, _Ephemeral
        gc.collect()

        # Assert
        assert reference() is None


class TestSharedSubscription:
    """Tests for _SharedSubscription class.

    Fully qualified name: wool.runtime.discovery.pool._SharedSubscription
    """

    @pytest.mark.asyncio
    async def test___aiter___with_demand_driven_pull(self):
        """Test iteration pulls from the source.

        Given:
            A _SharedSubscription backed by a source that yields
            one event.
        When:
            The subscription is iterated.
        Then:
            It should return the event from the source.
        """
        # Arrange
        event = _make_event()

        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield event

        shared = _make_shared(_Source())

        # Act
        result = await anext(aiter(shared))

        # Assert
        assert result is event

    @pytest.mark.asyncio
    async def test___aiter___with_fan_out_to_multiple_consumers(self):
        """Test events are fanned out to all iterators.

        Given:
            Two iterators from subscriptions sharing the same key,
            both initialised.
        When:
            One iterator pulls a new event.
        Then:
            The other should receive the same event via fan-out.
        """
        # Arrange
        events = [_make_event(address=f"127.0.0.1:{50051 + i}") for i in range(3)]

        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                for e in events:
                    yield e

        _setup_pool()
        source = _Source()
        key = _SubscriberKey("fan-out-key", lambda: source)

        sub_a = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))
        sub_b = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))
        it_a = aiter(sub_a)
        it_b = aiter(sub_b)

        # Initialise both consumers
        await anext(it_a)
        await anext(it_b)

        # Act — b pulls events[1] from source, fans out to a
        result_b = await anext(it_b)
        result_a = await anext(it_a)

        # Assert — same object via fan-out
        assert result_a is result_b
        assert result_a is events[1]

    @pytest.mark.asyncio
    async def test___aiter___with_independent_iterators(self):
        """Test each __aiter__ call returns a distinct iterator.

        Given:
            A _SharedSubscription instance.
        When:
            __aiter__ is called twice.
        Then:
            It should return two distinct async generator instances.
        """
        # Arrange
        shared = _StubSubscriber("iter-key")

        # Act
        a = aiter(shared)
        b = aiter(shared)

        # Assert
        assert a is not b

    @pytest.mark.asyncio
    async def test___aiter___with_resource_release_on_exhaustion(self):
        """Test the pool resource is released when the source exhausts.

        Given:
            A _SharedSubscription backed by a finite source.
        When:
            The subscription is iterated to exhaustion.
        Then:
            The pool resource should be released.
        """

        # Arrange
        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield _make_event()

        shared = _make_shared(_Source())

        # Act
        collected = [event async for event in shared]

        # Assert
        assert len(collected) == 1
        pool = __subscriber_pool__.get()
        assert pool.stats.referenced_entries == 0

    def test___reduce___pickle_roundtrip(self):
        """Test _SharedSubscription pickle roundtrip via cloudpickle.

        Given:
            A _SharedSubscription wrapping a stub subscriber with
            __reduce__.
        When:
            The subscription is pickled and unpickled.
        Then:
            It should produce a _SharedSubscription wrapping a
            subscriber with the same key.
        """
        # Arrange
        original = _StubSubscriber("pickle-key")

        # Act
        pickled = cloudpickle.dumps(original)
        restored = cloudpickle.loads(pickled)

        # Assert
        assert isinstance(restored, _SharedSubscription)

    @pytest.mark.asyncio
    async def test___aiter___with_late_joiner_replay(self):
        """Test late-joining consumer receives replayed worker state.

        Given:
            A subscription that has pulled two events, tracking
            two workers.
        When:
            A second subscription with the same key starts
            iterating while the first is still active.
        Then:
            The second should receive replayed worker-added events
            for all tracked workers.
        """
        # Arrange
        events = [_make_event(address=f"127.0.0.1:{50051 + i}") for i in range(2)]

        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                for e in events:
                    yield e

        _setup_pool()
        source = _Source()
        key = _SubscriberKey("replay-key", lambda: source)

        sub_a = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))
        sub_b = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))

        # A pulls both events, tracking two workers.
        it_a = aiter(sub_a)
        await anext(it_a)
        await anext(it_a)

        # Act — B joins while A is still active, gets replay.
        collected_b = [event async for event in sub_b]

        # Assert — B receives replayed worker-added events.
        assert len(collected_b) == 2
        assert all(e.type == "worker-added" for e in collected_b)
        expected_uids = {str(e.metadata.uid) for e in events}
        actual_uids = {str(e.metadata.uid) for e in collected_b}
        assert actual_uids == expected_uids

    @pytest.mark.asyncio
    async def test___aiter___with_no_pool(self):
        """Test iteration raises RuntimeError when pool is absent.

        Given:
            A _SharedSubscription constructed directly with no
            subscriber pool initialised.
        When:
            The subscription is iterated.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        __subscriber_pool__.set(None)
        shared = _SharedSubscription(key="no-pool", reduce_info=(object, (), {}))

        # Act & assert
        with pytest.raises(RuntimeError, match="subscriber pool not initialised"):
            await anext(aiter(shared))

    @pytest.mark.asyncio
    async def test___aiter___with_worker_dropped_tracking(self):
        """Test worker-dropped events clear tracked worker state.

        Given:
            A subscription whose source yields worker-added then
            worker-dropped events for the same worker.
        When:
            A second subscription starts iterating after the first
            has processed both events.
        Then:
            The late joiner should receive no replay because the
            worker was dropped.
        """
        # Arrange
        worker_uid = __import__("uuid").uuid4()
        metadata = WorkerMetadata(
            uid=worker_uid,
            address="127.0.0.1:50051",
            pid=1,
            version="1.0.0",
        )
        add_event = DiscoveryEvent("worker-added", metadata=metadata)
        drop_event = DiscoveryEvent("worker-dropped", metadata=metadata)

        class _Source:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                yield add_event
                yield drop_event

        _setup_pool()
        source = _Source()
        key = _SubscriberKey("drop-key", lambda: source)

        sub_a = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))
        sub_b = _SharedSubscription(key=key, reduce_info=(type(source), (), {}))

        # A pulls both events — worker added then dropped.
        it_a = aiter(sub_a)
        await anext(it_a)
        await anext(it_a)

        # Act — B joins; worker was dropped so no replay.
        collected_b = [event async for event in sub_b]

        # Assert — B receives nothing (worker was removed).
        assert collected_b == []

    @pytest.mark.asyncio
    async def test___aiter___should_survive_a_peer_iterations_cancellation(self):
        """Test cancelling one iteration leaves its peers subscribed.

        Given:
            Two iterations of one shared subscription over a source that
            suspends before its first event, the first parked on a pull
        When:
            That first iteration is cancelled and the source then yields
        Then:
            It should deliver the event to the second iteration, one
            consumer's teardown not being the end of a feed its peers
            still hold.
        """
        # Arrange
        release = asyncio.Event()

        class _Suspending(
            metaclass=SubscriberMeta,
            key=lambda cls, tag: (cls, tag),
        ):
            def __init__(self, tag: str) -> None:
                self.tag = tag

            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                await release.wait()
                while True:
                    yield _make_event()

        shared = _Suspending("cancelled-peer-key")
        first = aiter(shared)
        second = aiter(shared)
        parked = asyncio.ensure_future(anext(first))
        waiting = asyncio.ensure_future(anext(second))
        # Both iterations are on the shared source before either is
        # cancelled, so the survivor is a genuine peer.
        await asyncio.sleep(0.05)
        assert not parked.done()
        assert not waiting.done()

        # Act
        parked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await parked
        release.set()

        # Assert
        event = await asyncio.wait_for(waiting, timeout=2)
        assert event.type == "worker-added"

    @pytest.mark.asyncio
    async def test___aiter___should_raise_stop_async_iteration_when_pool_cleared(
        self,
    ):
        """Test iteration ends once the pool finalizes the shared subscriber.

        Given:
            A subscription whose iterator is parked on an endless
            source, holding the pool's only reference to the subscriber
            behind it.
        When:
            The subscriber pool is cleared on the loop that cached it.
        Then:
            It should raise StopAsyncIteration on the next pull, the
            finalized subscriber's fan-out having no events left to
            hand out.
        """
        # Arrange
        shared = _numbered_subscriber_class()("cleanup-key")
        it = aiter(shared)
        await anext(it)  # initialise the fanout
        pool = __subscriber_pool__.get()

        # Act -- clear force-finalizes the cached subscriber, cleaning
        # up its fan-out, even though the parked iterator still holds a
        # reference to it.
        await pool.clear()

        # Assert
        with pytest.raises(StopAsyncIteration):
            await anext(it)

    @pytest.mark.asyncio
    async def test___aiter___should_build_a_subscriber_per_loop_when_iterated_on_two(
        self, background_loops
    ):
        """Test one cache key is served by one subscriber per iterating loop.

        Given:
            A subscription for a single key whose subscriber instances
            number themselves, already iterated and still referenced on
            the test loop.
        When:
            The same subscription is iterated on a background loop.
        Then:
            It should serve that loop from a second subscriber instance,
            leaving the test loop's own entry cached, since the pool
            caches per loop and not per key alone.
        """
        # Arrange
        shared = _numbered_subscriber_class()("two-loop-key")
        pool = __subscriber_pool__.get()
        background = background_loops(teardown=lambda: pool.clear())
        it = aiter(shared)
        local = await anext(it)

        async def first_event(pool, shared):
            # ``run_coroutine_threadsafe`` does not carry the caller's
            # context, so the pool is handed over and installed here.
            __subscriber_pool__.set(pool)
            assert __subscriber_pool__.get() is pool
            return await anext(aiter(shared))

        # Act
        remote = background.run(first_event(pool, shared))

        # Assert
        assert remote.metadata.address != local.metadata.address
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    async def test___aiter___should_keep_yielding_when_another_loop_clears_the_pool(
        self, background_loops
    ):
        """Test a clear on one loop leaves another loop's subscriber iterable.

        Given:
            One subscription iterated on both the test loop and a
            background loop, each served by its own subscriber instance.
        When:
            The subscriber pool is cleared on the test loop.
        Then:
            It should leave the background loop's iterator yielding from
            the subscriber it already had, the clear having reached only
            the partition of the loop that ran it.
        """
        # Arrange
        shared = _numbered_subscriber_class()("clear-key")
        pool = __subscriber_pool__.get()
        background = background_loops(teardown=lambda: pool.clear())
        parked: dict = {}

        async def start_iterating(pool, shared, parked):
            # ``run_coroutine_threadsafe`` does not carry the caller's
            # context, so the pool is handed over and installed here.
            __subscriber_pool__.set(pool)
            assert __subscriber_pool__.get() is pool
            parked["iterator"] = aiter(shared)
            return await anext(parked["iterator"])

        async def next_event(pool, parked):
            __subscriber_pool__.set(pool)
            assert __subscriber_pool__.get() is pool
            return await anext(parked["iterator"])

        before = background.run(start_iterating(pool, shared, parked))
        await anext(aiter(shared))

        # Act
        await pool.clear()

        # Assert
        after = background.run(next_event(pool, parked))
        assert after.metadata.address == before.metadata.address
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test___aiter___should_rebuild_after_its_subscriber_retired(
        self, subscriber_pool
    ):
        """Test a subscription outlives the subscriber it built.

        Given:
            A subscription iterated as far as its first event and then
            closed, so the pool released its only reference and, having
            no TTL, finalized and evicted the subscriber behind it.
        When:
            The same subscription is iterated a second time.
        Then:
            It should build a second subscriber and yield from it, since
            a subscription holds no subscriber of its own and has to be
            able to rebuild one for as long as it is reachable.
        """
        # Arrange
        shared = _numbered_subscriber_class()("rebuild-key")
        first = aiter(shared)
        before = await anext(first)
        await first.aclose()
        assert subscriber_pool.stats.total_entries == 0

        # Act
        second = aiter(shared)
        after = await anext(second)

        # Assert
        assert before.metadata.address == "127.0.0.1:50051"
        assert after.metadata.address == "127.0.0.1:50052"
        await second.aclose()

    @pytest.mark.asyncio
    async def test___aiter___should_yield_when_a_peer_subscription_is_collected(
        self, subscriber_pool
    ):
        """Test one subscription does not depend on another's lifetime.

        Given:
            Two subscriptions constructed under one key, neither
            iterated.
        When:
            The first is dropped, a collection runs, and the second is
            iterated.
        Then:
            It should yield, since every subscription carries what it
            needs to build the subscriber rather than borrowing one
            registration the first construction happened to make.
        """
        # Arrange
        subscriber = _numbered_subscriber_class()
        first = subscriber("peer-key")
        second = subscriber("peer-key")

        # Act
        del first
        gc.collect()

        # Assert
        iterator = aiter(second)
        assert await anext(iterator)
        await iterator.aclose()
