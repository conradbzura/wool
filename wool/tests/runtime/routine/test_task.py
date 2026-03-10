import asyncio
import logging
from typing import Callable
from uuid import uuid4

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool import protocol
from wool.runtime.event import Event
from wool.runtime.routine.task import IterationEvent
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import TaskEvent
from wool.runtime.routine.task import TaskException
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.routine.task import current_task
from wool.runtime.routine.task import do_dispatch


class _PicklableProxy:
    """A simple picklable proxy for tests that cannot use fixtures."""

    def __init__(self):
        self.id = uuid4()

    async def dispatch(self, *args, **kwargs):
        async def _stream():
            yield "result"

        return _stream()


# --- Protocol conformance test classes ---


class TestWorkerProxyLike:
    """Tests for :py:class:`WorkerProxyLike` protocol conformance."""

    def test_positive_conformance(self, sample_task, clear_event_handlers):
        """
        Given:
            A class that implements the ``id`` property and async
            ``dispatch`` method required by :py:class:`WorkerProxyLike`
        When:
            An instance is passed as the ``proxy`` argument to
            :py:class:`Task`
        Then:
            The Task is instantiated successfully and the proxy's
            methods are callable.
        """

        # Arrange
        class ConformingProxy:
            @property
            def id(self):
                return uuid4()

            async def dispatch(self, task, *, timeout=None):
                yield "result"

        proxy = ConformingProxy()

        # Act
        task = sample_task(proxy=proxy)

        # Assert
        assert task.proxy is proxy
        assert hasattr(task.proxy, "id")
        assert callable(task.proxy.dispatch)

    def test_negative_conformance(self, sample_async_callable, clear_event_handlers):
        """
        Given:
            A class that has an ``id`` property but is missing the
            ``dispatch`` method required by
            :py:class:`WorkerProxyLike`
        When:
            An instance is passed as the ``proxy`` argument to
            :py:class:`Task`
        Then:
            ``TypeError`` is raised.
        """

        # Arrange
        class NonConformingProxy:
            @property
            def id(self):
                return uuid4()

        proxy = NonConformingProxy()

        # Act & Assert
        with pytest.raises(TypeError, match="proxy must conform to WorkerProxyLike"):
            Task(
                id=uuid4(),
                callable=sample_async_callable,
                args=(),
                kwargs={},
                proxy=proxy,
            )


class TestTaskEventHandler:
    """Tests for :py:class:`TaskEventHandler` protocol conformance."""

    def test_positive_conformance(self, sample_task, event_spy, clear_event_handlers):
        """
        Given:
            A callable with ``(event, timestamp, context=None)``
            signature registered via ``@TaskEvent.handler``
        When:
            An event of the registered type is emitted
        Then:
            The handler is called with the emitted event.
        """
        # Arrange
        call_log = []

        @TaskEvent.handler("task-created")
        def conforming_handler(event, timestamp, context=None):
            call_log.append((event, timestamp, context))

        task = sample_task()

        # Act
        event = TaskEvent("task-created", task=task)
        event.emit()
        Event.flush()

        # Assert
        assert len(call_log) >= 1
        emitted_event, timestamp, context = call_log[-1]
        assert emitted_event.type == "task-created"
        assert callable(conforming_handler)


# --- Module-level do_dispatch tests ---


def test_do_dispatch_with_default_context():
    """
    Given:
        No ``do_dispatch`` context manager is active
    When:
        ``do_dispatch()`` is called without arguments
    Then:
        Returns ``True``.
    """
    # Arrange & Act & Assert
    assert do_dispatch() is True


def test_do_dispatch_with_false_flag():
    """
    Given:
        A ``do_dispatch(False)`` context manager is active
    When:
        ``do_dispatch()`` is called inside the context
    Then:
        Returns ``False`` inside the context and ``True`` after
        exiting.
    """
    # Act & Assert
    with do_dispatch(False):
        assert not do_dispatch()
    assert do_dispatch()


def test_do_dispatch_with_nested_contexts():
    """
    Given:
        Nested ``do_dispatch`` context managers with outer ``True``
        and inner ``False``
    When:
        ``do_dispatch()`` is called at each level
    Then:
        Returns the value matching the innermost active context and
        restores correctly on exit.
    """
    # Arrange & Act & Assert
    with do_dispatch(True):
        assert do_dispatch() is True
        with do_dispatch(False):
            assert do_dispatch() is False
        assert do_dispatch() is True
    assert do_dispatch() is True


# --- Module-level current_task tests ---


@pytest.mark.asyncio
async def test_current_task_inside_task_context(
    sample_task, mock_worker_proxy_cache, clear_event_handlers
):
    """
    Given:
        Execution is within a task context via ``dispatch()``
    When:
        ``current_task()`` is called inside the task callable
    Then:
        Returns the current :py:class:`Task` instance.
    """

    # Arrange
    async def test_callable():
        return current_task()

    task = sample_task(callable=test_callable)

    # Act
    result = await task.dispatch()

    # Assert
    assert result == task


def test_current_task_outside_task_context():
    """
    Given:
        Execution is outside any task context
    When:
        ``current_task()`` is called
    Then:
        Returns ``None``.
    """
    # Arrange & Act
    result = current_task()

    # Assert
    assert result is None


@pytest.mark.asyncio
async def test_current_task_with_nested_task_contexts(sample_task, clear_event_handlers):
    """
    Given:
        An outer task entered via ``with outer_task:``
    When:
        An inner task is created inside the outer task's context
    Then:
        The inner task's ``caller`` is set to the outer task's ``id``.
    """
    # Arrange
    outer_task = sample_task()

    # Act
    with outer_task:
        inner_task = sample_task()

    # Assert
    assert inner_task.caller == outer_task.id


# --- TestTask ---


class TestTask:
    """Tests for :py:class:`Task`."""

    def test___post_init___with_event_handler(
        self, sample_task, event_spy, clear_event_handlers
    ):
        """
        Given:
            A handler registered for ``"task-created"`` via
            ``@TaskEvent.handler``
        When:
            A :py:class:`Task` is instantiated
        Then:
            The handler is called with a ``"task-created"`` event.
        """

        # Arrange
        @TaskEvent.handler("task-created")
        def handler(event, timestamp, context=None):
            event_spy(event, timestamp, context)

        # Act
        task = sample_task()

        # Wait for handler thread to process scheduled handlers
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        event, timestamp, context = event_spy.calls[0]
        assert event.type == "task-created"
        assert event.task.id == task.id
        assert isinstance(timestamp, int)
        assert timestamp > 0

    @pytest.mark.asyncio
    async def test___post_init___inside_task_context(
        self, sample_task, clear_event_handlers
    ):
        """
        Given:
            An outer task entered via ``with outer_task:``
        When:
            A new task is created inside the outer task's context
        Then:
            The inner task's ``caller`` is set to the outer task's
            ``id``.
        """
        # Arrange
        outer_task = sample_task()

        # Act
        with outer_task:
            inner_task = sample_task()

        # Assert
        assert inner_task.caller == outer_task.id

    def test___post_init___outside_task_context(self, sample_task, clear_event_handlers):
        """
        Given:
            No task context is active
        When:
            A :py:class:`Task` is instantiated
        Then:
            The ``caller`` field remains ``None``.
        """
        # Arrange & Act
        task = sample_task()

        # Assert
        assert task.caller is None

    @pytest.mark.asyncio
    async def test___enter___with_coroutine_callable(
        self, sample_task, clear_event_handlers
    ):
        """
        Given:
            A :py:class:`Task` with a coroutine callable
        When:
            The task is used as a context manager
        Then:
            ``__enter__`` returns a callable.
        """
        # Arrange
        task = sample_task()

        # Act
        with task as run_method:
            # Assert
            assert callable(run_method)

    @pytest.mark.asyncio
    async def test___exit___without_exception(self, sample_task, clear_event_handlers):
        """
        Given:
            A :py:class:`Task` context manager
        When:
            The ``with`` block completes without exception
        Then:
            The context exits cleanly.
        """
        # Arrange
        task = sample_task()

        # Act & Assert (no exception should be raised)
        with task:
            pass

    @pytest.mark.asyncio
    async def test___exit___with_value_error(self, sample_task, clear_event_handlers):
        """
        Given:
            A :py:class:`Task` context manager
        When:
            A ``ValueError`` is raised inside the ``with`` block
        Then:
            A :py:class:`TaskException` is attached to the task and
            the exception propagates.
        """
        # Arrange
        task = sample_task()

        # Act & Assert
        async def run_with_exception():
            with pytest.raises(ValueError, match="test error"):
                with task:
                    await asyncio.sleep(0)
                    raise ValueError("test error")

        await run_with_exception()

        # Assert that exception was captured
        assert task.exception is not None
        assert task.exception.type == "ValueError"
        assert any("test error" in line for line in task.exception.traceback)

    @pytest.mark.asyncio
    async def test___exit___with_runtime_error(self, sample_task, clear_event_handlers):
        """
        Given:
            A :py:class:`Task` context manager
        When:
            A ``RuntimeError`` is raised inside the ``with`` block
        Then:
            A :py:class:`TaskException` is attached to the task with
            the correct type and the exception propagates.
        """
        # Arrange
        task = sample_task()

        # Act
        async def run_with_exception():
            with pytest.raises(RuntimeError, match="runtime error"):
                with task:
                    await asyncio.sleep(0)
                    raise RuntimeError("runtime error")

        await run_with_exception()

        # Assert
        assert task.exception is not None
        assert task.exception.type == "RuntimeError"
        assert any("runtime error" in line for line in task.exception.traceback)

    @pytest.mark.asyncio
    async def test___enter___with_async_generator(
        self, sample_task, clear_event_handlers
    ):
        """
        Given:
            A :py:class:`Task` with an async generator callable
        When:
            The task is used as a context manager
        Then:
            ``__enter__`` returns a callable.
        """

        # Arrange
        async def test_generator():
            yield "value"

        task = sample_task(callable=test_generator)

        # Act
        with task as run_method:
            # Assert
            assert callable(run_method)

    @pytest.mark.asyncio
    async def test___enter___with_invalid_callable(
        self, sample_task, clear_event_handlers
    ):
        """
        Given:
            A :py:class:`Task` with neither a coroutine nor an async
            generator callable
        When:
            The task is used as a context manager
        Then:
            ``ValueError`` is raised.
        """

        # Arrange
        def not_async():
            return "not async"

        task = sample_task(callable=not_async)

        # Act & Assert
        with pytest.raises(
            ValueError,
            match="Expected coroutine function or async generator function",
        ):
            with task:
                pass

    @settings(max_examples=50, deadline=None)
    @given(
        task_id=st.uuids(),
        timeout=st.integers(min_value=0, max_value=3600),
        caller_id=st.one_of(st.none(), st.uuids()),
        tag=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    )
    @pytest.mark.asyncio
    async def test_to_protobuf_with_picklable_proxy(
        self,
        task_id,
        timeout,
        caller_id,
        tag,
    ):
        """
        Given:
            A :py:class:`Task` with valid picklable data
        When:
            ``from_protobuf(to_protobuf(task))`` is called
        Then:
            The deserialized task equals the original in all public
            attributes.
        """

        # Arrange
        async def test_callable():
            return "result"

        proxy = _PicklableProxy()
        args = (1, "test", [1, 2, 3])
        kwargs = {"key": "value", "number": 42}

        original_task = Task(
            id=task_id,
            callable=test_callable,
            args=args,
            kwargs=kwargs,
            proxy=proxy,
            timeout=timeout,
            caller=caller_id,
            tag=tag,
        )

        # Act
        pb_task = original_task.to_protobuf()
        deserialized_task = Task.from_protobuf(pb_task)

        # Assert
        assert deserialized_task.id == original_task.id
        assert callable(deserialized_task.callable)
        assert deserialized_task.callable.__name__ == original_task.callable.__name__
        assert deserialized_task.args == original_task.args
        assert deserialized_task.kwargs == original_task.kwargs
        assert deserialized_task.caller == original_task.caller
        assert deserialized_task.proxy.id == original_task.proxy.id
        assert deserialized_task.timeout == original_task.timeout
        assert deserialized_task.tag == original_task.tag

    @pytest.mark.asyncio
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(value_count=st.integers(min_value=0, max_value=10))
    async def test_dispatch_with_async_generator(
        self,
        value_count,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """
        Given:
            An async generator yielding *N* values (0 <= N <= 10)
        When:
            Dispatched via ``dispatch()`` and fully consumed
        Then:
            All *N* values are received in order.
        """

        # Arrange
        async def test_generator():
            for i in range(value_count):
                yield i

        proxy = _PicklableProxy()
        task = Task(
            id=uuid4(),
            callable=test_generator,
            args=(),
            kwargs={},
            proxy=proxy,
        )

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)

        # Assert
        assert len(results) == value_count
        assert results == list(range(value_count))

    @pytest.mark.asyncio
    async def test_from_protobuf_all_fields(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """
        Given:
            A protobuf Task message with all fields populated
        When:
            ``from_protobuf`` is called
        Then:
            Returns a :py:class:`Task` with all fields correctly
            deserialized.
        """
        # Arrange
        task_id = uuid4()
        caller_id = uuid4()
        args = (1, 2, 3)
        kwargs = {"key": "value"}

        pb_task = protocol.task.Task(
            version="0.1.0",
            id=str(task_id),
            callable=cloudpickle.dumps(sample_async_callable),
            args=cloudpickle.dumps(args),
            kwargs=cloudpickle.dumps(kwargs),
            caller=str(caller_id),
            proxy=cloudpickle.dumps(picklable_proxy),
            proxy_id=str(picklable_proxy.id),
            timeout=30,
            tag="test_tag",
        )

        # Act
        task = Task.from_protobuf(pb_task)

        # Assert
        assert task.id == task_id
        assert callable(task.callable)
        assert task.callable.__name__ == sample_async_callable.__name__
        assert task.args == args
        assert task.kwargs == kwargs
        assert task.caller == caller_id
        assert task.proxy.id == picklable_proxy.id
        assert task.timeout == 30
        assert task.tag == "test_tag"

    @pytest.mark.asyncio
    async def test_from_protobuf_empty_optionals(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """
        Given:
            A protobuf Task message with optional fields empty
        When:
            ``from_protobuf`` is called
        Then:
            Returns a :py:class:`Task` with ``None`` or ``0`` for
            empty optional fields.
        """
        # Arrange
        task_id = uuid4()
        args = ()
        kwargs = {}

        pb_task = protocol.task.Task(
            id=str(task_id),
            callable=cloudpickle.dumps(sample_async_callable),
            args=cloudpickle.dumps(args),
            kwargs=cloudpickle.dumps(kwargs),
            caller="",
            proxy=cloudpickle.dumps(picklable_proxy),
            proxy_id=str(picklable_proxy.id),
            timeout=0,
            tag="",
        )

        # Act
        task = Task.from_protobuf(pb_task)

        # Assert
        assert task.id == task_id
        assert task.caller is None
        assert task.timeout == 0
        assert task.tag is None

    def test_to_protobuf_all_fields(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """
        Given:
            A :py:class:`Task` instance with all fields populated
        When:
            ``to_protobuf()`` is called
        Then:
            Returns a protobuf Task with all fields correctly
            serialized.
        """
        # Arrange
        caller_id = uuid4()
        task_id = uuid4()
        task = Task(
            id=task_id,
            callable=sample_async_callable,
            args=(1, 2),
            kwargs={"key": "value"},
            proxy=picklable_proxy,
            caller=caller_id,
            timeout=30,
            tag="test_tag",
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
        assert pb_task.version != ""
        assert pb_task.id == str(task.id)
        deserialized_callable = cloudpickle.loads(pb_task.callable)
        assert callable(deserialized_callable)
        assert deserialized_callable.__name__ == task.callable.__name__
        assert cloudpickle.loads(pb_task.args) == task.args
        assert cloudpickle.loads(pb_task.kwargs) == task.kwargs
        assert pb_task.caller == str(caller_id)
        deserialized_proxy = cloudpickle.loads(pb_task.proxy)
        assert deserialized_proxy.id == task.proxy.id
        assert pb_task.proxy_id == str(task.proxy.id)
        assert pb_task.timeout == 30
        assert pb_task.tag == "test_tag"

    def test_to_protobuf_none_optionals(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """
        Given:
            A :py:class:`Task` instance with optional fields as
            ``None`` or ``0``
        When:
            ``to_protobuf()`` is called
        Then:
            Returns a protobuf Task with empty strings and ``0`` for
            optional fields.
        """
        # Arrange
        task = Task(
            id=uuid4(),
            callable=sample_async_callable,
            args=(),
            kwargs={},
            proxy=picklable_proxy,
            caller=None,
            timeout=0,
            tag=None,
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
        assert pb_task.caller == ""
        assert pb_task.timeout == 0
        assert pb_task.tag == ""

    def test_to_protobuf_with_version_field(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """
        Given:
            A :py:class:`Task` instance
        When:
            ``to_protobuf()`` is called
        Then:
            The protobuf Task contains the protocol version in the
            ``version`` field.
        """
        # Arrange
        task = Task(
            id=uuid4(),
            callable=sample_async_callable,
            args=(),
            kwargs={},
            proxy=picklable_proxy,
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
        assert pb_task.version == protocol.__version__

    @settings(
        max_examples=50,
        deadline=None,
    )
    @given(
        version=st.from_regex(r"\d{1,3}\.\d{1,3}(rc\d{1,3}|\.\d{1,3})", fullmatch=True),
    )
    def test_from_protobuf_with_version_roundtrip(self, version):
        """
        Given:
            Any PEP 440-like version string
        When:
            A protobuf Task with that version is serialized and
            deserialized
        Then:
            The version field is preserved on the wire.
        """
        # Arrange
        proxy = _PicklableProxy()

        async def test_callable():
            return "result"

        pb_task = protocol.task.Task(
            version=version,
            id=str(uuid4()),
            callable=cloudpickle.dumps(test_callable),
            args=cloudpickle.dumps(()),
            kwargs=cloudpickle.dumps({}),
            caller="",
            proxy=cloudpickle.dumps(proxy),
            proxy_id=str(proxy.id),
            timeout=0,
            tag="",
        )

        # Act
        wire_bytes = pb_task.SerializeToString()
        parsed = protocol.task.Task()
        parsed.ParseFromString(wire_bytes)

        # Assert
        assert parsed.version == version

    @pytest.mark.asyncio
    async def test_dispatch_successful_execution(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with a valid proxy pool in context
        When:
            ``dispatch()`` is called
        Then:
            Executes the callable and returns the result.
        """

        # Arrange
        async def test_callable(x, y=0):
            return x + y

        task = sample_task(
            callable=test_callable,
            args=(5,),
            kwargs={"y": 3},
        )

        # Act
        result = await task.dispatch()

        # Assert
        assert result == 8

    @pytest.mark.asyncio
    async def test_dispatch_emits_task_completed_event_on_success(
        self,
        sample_task,
        mock_worker_proxy_cache,
        event_spy,
        clear_event_handlers,
    ):
        """
        Given:
            A handler registered for ``"task-completed"``
        When:
            A task completes execution successfully
        Then:
            A ``"task-completed"`` event is emitted.
        """

        # Arrange
        @TaskEvent.handler("task-completed")
        def handler(event, timestamp, context=None):
            event_spy(event, timestamp, context)

        async def test_callable():
            return 42

        task = sample_task(callable=test_callable)

        # Act
        task_handle = asyncio.create_task(task.dispatch())
        result = await task_handle

        # Wait for the done callback to be invoked
        await asyncio.sleep(0.01)

        # Assert
        assert result == 42
        assert len(event_spy.calls) == 1
        event, timestamp, context = event_spy.calls[0]
        assert event.type == "task-completed"
        assert event.task.id == task.id
        assert event.task.exception is None

    @pytest.mark.asyncio
    async def test_dispatch_emits_task_completed_event_on_error(
        self,
        sample_task,
        mock_worker_proxy_cache,
        event_spy,
        clear_event_handlers,
    ):
        """
        Given:
            A handler registered for ``"task-completed"``
        When:
            A task completes execution with a ``ValueError``
        Then:
            A ``"task-completed"`` event is emitted with exception
            information attached.
        """

        # Arrange
        @TaskEvent.handler("task-completed")
        def handler(event, timestamp, context=None):
            event_spy(event, timestamp, context)

        async def test_callable():
            raise ValueError("Test error")

        task = sample_task(callable=test_callable)

        # Act
        async def run_task():
            try:
                await task.dispatch()
            except ValueError:
                pass

        task_handle = asyncio.create_task(run_task())
        await task_handle

        # Wait for the callback to be invoked
        await asyncio.sleep(0.01)

        # Assert
        assert len(event_spy.calls) == 1
        event, timestamp, context = event_spy.calls[0]
        assert event.type == "task-completed"
        assert event.task.id == task.id
        assert event.task.exception is not None

    @pytest.mark.asyncio
    async def test_dispatch_without_proxy_pool_raises_error(
        self, sample_task, clear_event_handlers
    ):
        """
        Given:
            No proxy pool is set in the context
        When:
            ``dispatch()`` is called
        Then:
            ``RuntimeError`` is raised.
        """
        # Arrange
        task = sample_task()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match="No proxy pool available for task execution",
        ):
            await task.dispatch()

    def test_to_protobuf_with_unpicklable_callable_fails(
        self, picklable_proxy, clear_event_handlers
    ):
        """
        Given:
            A :py:class:`Task` with an unpicklable callable
        When:
            ``to_protobuf()`` is called
        Then:
            Pickling fails with ``TypeError`` or ``AttributeError``.
        """
        # Arrange
        import _thread

        unpicklable_obj = _thread.allocate_lock()

        async def unpicklable_callable():
            return unpicklable_obj

        unpicklable_callable.__qualname__ = "unpicklable_callable"

        task = Task(
            id=uuid4(),
            callable=unpicklable_callable,
            args=(),
            kwargs={},
            proxy=picklable_proxy,
        )

        # Act & Assert
        with pytest.raises((TypeError, AttributeError)):
            task.to_protobuf()

    @pytest.mark.asyncio
    async def test_dispatch_with_async_generator_callable(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with an async generator callable and a
            valid proxy pool
        When:
            ``dispatch()`` is called and iterated
        Then:
            Yields all values from the async generator in order.
        """

        # Arrange
        async def test_generator():
            for i in range(3):
                yield f"value_{i}"

        task = sample_task(callable=test_generator)

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)

        # Assert
        assert results == ["value_0", "value_1", "value_2"]

    @pytest.mark.asyncio
    async def test_dispatch_with_coroutine_callable(
        self,
        sample_task: Callable[..., Task],
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with a coroutine callable and a valid
            proxy pool
        When:
            ``dispatch()`` is called and awaited
        Then:
            Returns the result from the coroutine.
        """

        # Arrange
        async def test_coroutine():
            return "coroutine_result"

        task = sample_task(callable=test_coroutine)

        # Act
        result = await task.dispatch()

        # Assert
        assert result == "coroutine_result"

    @pytest.mark.asyncio
    async def test_dispatch_with_invalid_callable_raises_error(
        self,
        sample_task,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with a non-async callable
        When:
            ``dispatch()`` is called
        Then:
            ``ValueError`` is raised.
        """

        # Arrange
        def not_async():
            return "not async"

        task = sample_task(callable=not_async)

        # Act & Assert
        with pytest.raises(
            ValueError,
            match="Expected routine to be coroutine or async generator",
        ):
            _ = task.dispatch()

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_without_proxy_pool_raises_error(
        self,
        sample_task,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with an async generator callable and no
            proxy pool in context
        When:
            ``dispatch()`` is called
        Then:
            ``RuntimeError`` is raised.
        """

        # Arrange
        async def test_generator():
            yield "value"

        task = sample_task(callable=test_generator)

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match="No proxy pool available for task execution",
        ):
            async for _ in task.dispatch():
                pass

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_raises_during_iteration(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with an async generator that raises
            during iteration
        When:
            ``dispatch()`` is called and iterated
        Then:
            The exception propagates to the caller.
        """

        # Arrange
        async def failing_generator():
            yield "first"
            raise ValueError("Generator error")

        task = sample_task(callable=failing_generator)

        # Act & Assert
        results = []
        with pytest.raises(ValueError, match="Generator error"):
            async for value in task.dispatch():
                results.append(value)

        # Verify we got the first value before the exception
        assert results == ["first"]

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_early_termination(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with an async generator callable
        When:
            The async iterator is terminated early via ``break``
        Then:
            Iteration stops after receiving expected values.
        """

        # Arrange
        async def test_generator():
            for i in range(10):
                yield f"value_{i}"

        task = sample_task(callable=test_generator)

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)
            if len(results) >= 2:
                break

        # Assert
        assert results == ["value_0", "value_1"]

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_multiple_values(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with an async generator that yields
            multiple values
        When:
            ``dispatch()`` is fully consumed
        Then:
            All yielded values are received in correct order.
        """

        # Arrange
        async def multi_value_generator():
            for i in range(5):
                await asyncio.sleep(0)
                yield i * 10

        task = sample_task(callable=multi_value_generator)

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)

        # Assert
        assert results == [0, 10, 20, 30, 40]

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_empty(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """
        Given:
            A :py:class:`Task` with an async generator that yields
            zero values
        When:
            ``dispatch()`` is called and consumed
        Then:
            Completes immediately without yielding any values.
        """

        # Arrange
        async def empty_generator():
            return
            yield  # unreachable, but makes it a generator

        task = sample_task(callable=empty_generator)

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)

        # Assert
        assert results == []


# --- TestTaskException ---


class TestTaskException:
    """Tests for :py:class:`TaskException`."""

    def test___init___with_type_and_traceback(self):
        """
        Given:
            A valid exception type string and traceback lines
        When:
            :py:class:`TaskException` is instantiated
        Then:
            The object stores the correct ``type`` and ``traceback``
            attributes.
        """
        # Arrange
        exception_type = "ValueError"
        traceback_lines = ["line1", "line2", "line3"]

        # Act
        exception = TaskException(
            type=exception_type,
            traceback=traceback_lines,
        )

        # Assert
        assert exception.type == exception_type
        assert exception.traceback == traceback_lines


# --- TestTaskEvent ---


class TestTaskEvent:
    """Tests for :py:class:`TaskEvent`."""

    def test___init___with_type_and_task(self, sample_task, clear_event_handlers):
        """
        Given:
            A valid event type and a :py:class:`Task`
        When:
            :py:class:`TaskEvent` is instantiated
        Then:
            The event stores the correct ``type`` and ``task``
            attributes.
        """
        # Arrange
        task = sample_task()

        # Act
        event = TaskEvent("task-created", task=task)

        # Assert
        assert event.type == "task-created"
        assert event.task == task

    def test_handler_with_single_event_type(self, sample_task, clear_event_handlers):
        """
        Given:
            A handler registered via ``@TaskEvent.handler`` for
            ``"task-created"`` and ``"task-completed"``
        When:
            Events of those types are emitted
        Then:
            The handler is called once per emitted event type.
        """
        # Arrange
        call_log = []

        @TaskEvent.handler("task-created", "task-completed")
        def test_handler(event, timestamp, context=None):
            call_log.append((event, timestamp, context))

        task = sample_task()

        # Act
        TaskEvent("task-created", task=task).emit()
        TaskEvent("task-completed", task=task).emit()
        Event.flush()

        # Assert
        assert len(call_log) >= 2
        event_types = [entry[0].type for entry in call_log]
        assert "task-created" in event_types
        assert "task-completed" in event_types
        assert callable(test_handler)

    def test_emit_with_event_handler(self, sample_task, event_spy, clear_event_handlers):
        """
        Given:
            A handler registered via ``@TaskEvent.handler`` for
            ``"task-created"``
        When:
            ``emit()`` is called on a ``"task-created"`` event
        Then:
            The handler is called with the event and a timestamp.
        """
        # Arrange
        task = sample_task()
        task_event = TaskEvent("task-created", task=task)

        @TaskEvent.handler("task-created")
        def handler(event, timestamp, context=None):
            event_spy(event, timestamp, context)

        # Act
        task_event.emit()
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        emitted_event, timestamp, context = event_spy.calls[0]
        assert emitted_event == task_event
        assert isinstance(timestamp, int)
        assert timestamp > 0

    def test_emit_without_event_handler(self, sample_task, clear_event_handlers):
        """
        Given:
            No handlers are registered
        When:
            ``emit()`` is called on a :py:class:`TaskEvent`
        Then:
            Execution completes normally without error.
        """
        # Arrange
        task = sample_task()
        event = TaskEvent("task-created", task=task)

        # Act & Assert (no exception should be raised)
        event.emit()

    def test_emit_with_raising_handler(self, sample_task, clear_event_handlers, caplog):
        """
        Given:
            A handler registered via ``@TaskEvent.handler`` that
            raises a ``ValueError``
        When:
            ``emit()`` is called
        Then:
            The exception is logged and does not propagate to the
            caller.
        """
        # Arrange
        task = sample_task()
        task_event = TaskEvent("task-created", task=task)

        @TaskEvent.handler("task-created")
        def failing_handler(event, timestamp, context=None):
            raise ValueError("Handler failed")

        # Act
        with caplog.at_level(logging.ERROR):
            task_event.emit()
            Event.flush()

        # Assert
        assert any(
            "Exception in event handler" in record.message for record in caplog.records
        )

    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        event_types_to_register=st.sets(
            st.sampled_from(
                [
                    "task-created",
                    "task-scheduled",
                    "task-started",
                    "task-stopped",
                    "task-completed",
                ]
            ),
            min_size=1,
            max_size=6,
        ),
        num_handlers=st.integers(min_value=1, max_value=5),
    )
    def test_emit_with_registered_handler(
        self,
        event_types_to_register,
        num_handlers,
        clear_event_handlers,
    ):
        """
        Given:
            Any set of event types and any number of handlers
        When:
            Handlers are registered via ``@TaskEvent.handler`` and
            events are emitted
        Then:
            All handlers are called exactly once for their registered
            event types.
        """

        # Arrange
        async def test_callable():
            return "result"

        proxy = _PicklableProxy()

        task = Task(
            id=uuid4(),
            callable=test_callable,
            args=(),
            kwargs={},
            proxy=proxy,
        )
        handler_calls = {i: [] for i in range(num_handlers)}

        for i in range(num_handlers):

            def create_handler(handler_id):
                def handler(event, timestamp, context=None):
                    handler_calls[handler_id].append((event, timestamp, context))

                return handler

            handler = create_handler(i)
            TaskEvent.handler(*event_types_to_register)(handler)

        # Act
        for event_type in event_types_to_register:
            event = TaskEvent(event_type, task=task)
            event.emit()

        Event.flush()

        # Assert
        for i in range(num_handlers):
            assert len(handler_calls[i]) == len(event_types_to_register)


# --- TestIterationEvent ---


class TestIterationEvent:
    """Tests for :py:class:`IterationEvent`."""

    def test___init___with_all_attributes(self, sample_task, clear_event_handlers):
        """
        Given:
            Valid parameters for an :py:class:`IterationEvent`
        When:
            An instance is created
        Then:
            It is an instance of :py:class:`TaskEvent`.
        """
        # Arrange
        task = sample_task()

        # Act
        event = IterationEvent(
            "task-iteration-initiated", task=task, kind="next", step=0
        )

        # Assert
        assert isinstance(event, TaskEvent)

    def test___init___with_kind_and_step(self, sample_task, clear_event_handlers):
        """
        Given:
            An :py:class:`IterationEvent` with ``kind="send"`` and
            ``step=3``
        When:
            Attributes are accessed
        Then:
            ``kind`` is ``"send"``, ``step`` is ``3``, ``type`` and
            ``task`` are set correctly.
        """
        # Arrange
        task = sample_task()

        # Act
        event = IterationEvent("task-iteration-started", task=task, kind="send", step=3)

        # Assert
        assert event.type == "task-iteration-started"
        assert event.task is task
        assert event.kind == "send"
        assert event.step == 3

    def test_emit_with_registered_handler(
        self, sample_task, event_spy, clear_event_handlers
    ):
        """
        Given:
            A handler registered via ``@TaskEvent.handler`` for
            ``"task-iteration-completed"``
        When:
            An :py:class:`IterationEvent` of that type is emitted
        Then:
            The handler receives the event with correct attributes.
        """

        # Arrange
        @TaskEvent.handler("task-iteration-completed")
        def handler(event, timestamp, context=None):
            event_spy(event, timestamp, context)

        task = sample_task()
        iteration_event = IterationEvent(
            "task-iteration-completed", task=task, kind="throw", step=5
        )

        # Act
        iteration_event.emit()
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        emitted_event, timestamp, context = event_spy.calls[0]
        assert emitted_event.type == "task-iteration-completed"
        assert emitted_event.kind == "throw"
        assert emitted_event.step == 5
        assert emitted_event.task is task
