import asyncio
import contextvars
import gc
import uuid
import weakref
from types import SimpleNamespace
from typing import Coroutine
from typing import cast

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import strategies as st

from tests.helpers import scoped_context
from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import ContextVarCollision
from wool.runtime.context import Token
from wool.runtime.context import copy_context
from wool.runtime.context import current_context
from wool.runtime.routine.task import Serializer

dumps = cloudpickle.dumps
loads = cloudpickle.loads


@pytest.fixture(autouse=True)
def isolated_context():
    with scoped_context():
        yield


class TestContextVar:
    def test___init___with_name_only(self):
        """Test ContextVar initialization with a name and no default.

        Given:
            A name string
        When:
            ContextVar is instantiated with the name
        Then:
            It should expose the name and raise LookupError on get() with no value set
        """
        # Act
        var = ContextVar("init_nameonly")

        # Assert
        assert var.name == "init_nameonly"
        with pytest.raises(LookupError):
            var.get()

    def test___init___with_default(self):
        """Test ContextVar initialization with a default value.

        Given:
            A name string and a default value
        When:
            ContextVar is instantiated with both
        Then:
            get() should return the default when no value is set
        """
        # Arrange
        var = ContextVar("init_withdefault", default=42)

        # Act & assert
        assert var.get() == 42

    def test___init___infers_namespace_from_caller(self):
        """Test ContextVar infers namespace from the caller's top-level package.

        Given:
            A ContextVar constructed from this test module
        When:
            No explicit namespace is provided
        Then:
            The namespace should be the top-level package of ``__name__``
        """
        # Arrange
        expected_ns = __name__.partition(".")[0]

        # Act
        var = ContextVar("inferred")

        # Assert
        assert var.namespace == expected_ns
        assert var.key == f"{expected_ns}:inferred"

    def test___init___accepts_explicit_namespace(self):
        """Test ContextVar uses an explicit namespace when provided.

        Given:
            A ContextVar constructed with namespace='myapp'
        When:
            No implicit inference is needed
        Then:
            The key should combine the explicit namespace with the name
        """
        # Act
        var = ContextVar("explicit", namespace="myapp")

        # Assert
        assert var.namespace == "myapp"
        assert var.key == "myapp:explicit"

    @given(
        name=st.text(
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            min_size=1,
            max_size=12,
        ),
        default_a=st.one_of(st.integers(), st.text(), st.booleans()),
        default_b=st.one_of(st.integers(), st.text(), st.booleans()),
    )
    def test___init___with_duplicate_key(self, name, default_a, default_b):
        """Test duplicate-key construction raises regardless of default match.

        Given:
            Any non-empty name and pair of default values
        When:
            A ContextVar is constructed, then a second with the identical key
        Then:
            The second construction raises ContextVarCollision whether or
            not the two defaults are equal
        """
        # A unique namespace per example guarantees the registry slot
        # is free without needing teardown between Hypothesis examples
        # (which share a single test-function invocation). The
        # collision still fires within the example because both
        # ContextVar calls use the same namespace + name.
        namespace = f"test_duplicate_{uuid.uuid4().hex}"

        # The registry holds vars weakly, so we hold ``first`` for the
        # duration of the second construction; without that pin the
        # weakref would drop before the second call and no collision
        # would fire.
        first = ContextVar(name, namespace=namespace, default=default_a)

        with pytest.raises(ContextVarCollision):
            ContextVar(name, namespace=namespace, default=default_b)

        # Hold ``first`` to the end so the collision actually had
        # something to collide with.
        assert first.key == f"{namespace}:{name}"

    def test___init___with_same_name_in_different_namespace(self):
        """Test duplicate names across different namespaces are allowed.

        Given:
            A ContextVar 'shared' in namespace 'lib_a'
        When:
            A second ContextVar 'shared' is constructed in namespace 'lib_b'
        Then:
            Both should register without collision
        """
        # Act
        a = ContextVar("shared", namespace="lib_a")
        b = ContextVar("shared", namespace="lib_b")

        # Assert
        assert a is not b
        assert a.key == "lib_a:shared"
        assert b.key == "lib_b:shared"

    def test_get_with_explicit_default_fallback(self):
        """Test ContextVar.get returns the supplied fallback when unset.

        Given:
            A ContextVar with no class-level default and no value set
        When:
            get() is called with a fallback argument
        Then:
            It should return the fallback argument
        """
        # Arrange
        var = ContextVar("no_default")

        # Act
        value = var.get("fallback")

        # Assert
        assert value == "fallback"

    def test_set_token_when_reset_from_first_set(self):
        """Test the first set's Token restores the var to its default on reset.

        Given:
            A ContextVar with a constructor default and a single set
            that captures a Token
        When:
            reset(token) is called
        Then:
            var.get() should return the constructor default
        """
        # Arrange
        var = ContextVar("restore_default", default="initial")
        token = var.set("x")

        # Act
        var.reset(token)

        # Assert
        assert var.get() == "initial"

    def test_set_token_restores_outer_value_when_reset_from_nested_set(self):
        """Test a nested set's Token restores the outer set's value on reset.

        Given:
            A ContextVar set to "outer" and then set again to "inner"
            capturing the inner Token
        When:
            reset(inner_token) is called
        Then:
            var.get() should return "outer" — Tokens stack, each
            restoring only the value replaced by its own set
        """
        # Arrange
        var = ContextVar("restore_outer", default="d")
        var.set("outer")
        inner_token = var.set("inner")

        # Act
        var.reset(inner_token)

        # Assert
        assert var.get() == "outer"

    def test_reset_with_used_token(self):
        """Test ContextVar.reset raises on a token already consumed.

        Given:
            A ContextVar and a Token that has already been used
        When:
            reset() is called with the same token again
        Then:
            It should raise RuntimeError
        """
        # Arrange
        var = ContextVar("once", default=0)
        token = var.set(1)
        var.reset(token)

        # Act & assert
        with pytest.raises(RuntimeError):
            var.reset(token)

    def test_reset_with_token_for_different_var(self):
        """Test ContextVar.reset rejects tokens minted by a different var.

        Given:
            Two distinct ContextVar instances, each with a set value
        When:
            reset() is called on one with the other's token
        Then:
            It should raise ValueError
        """
        # Arrange
        a = ContextVar("reset_a", default=0)
        b = ContextVar("reset_b", default=0)
        token = a.set(1)

        # Act & assert
        with pytest.raises(ValueError):
            b.reset(token)

    def test_reset_restores_old_value_in_different_context_scope(self):
        """Test reset restores old_value when invoked in a different Context scope.

        Given:
            A ContextVar set twice in the original Context, then a
            different wool.Context scope entered via Context.run
        When:
            reset(token) is invoked inside the scoped Context
        Then:
            The var should revert to the value captured in the token
        """
        # Arrange
        var = ContextVar("reset_fallback", default="initial")
        var.set("first")
        token = var.set("second")
        seeded = contextvars.copy_context()

        def body():
            var.set("outer-most")
            var.reset(token)
            return var.get()

        # Act
        result = seeded.run(body)

        # Assert
        assert result == "first"

    def test_reset_with_token_from_different_in_process_context(self):
        """Test reset rejects a token minted in a different in-process Context.

        Given:
            A ContextVar and a Token minted by var.set(...) inside
            ctx_a.run(...) — the token still holds its in-memory
            Context reference (distinct from the cross-process
            scenario covered elsewhere, which tests the UUID fallback
            after pickling)
        When:
            var.reset(token) is called inside ctx_b.run(...) where
            ctx_b is a different wool.Context in the same process
        Then:
            ValueError is raised with a message naming the different
            wool.Context — the in-process identity check fires, not
            the UUID fallback
        """
        # Arrange
        var = ContextVar("inprocess_reset", default="d")
        ctx_a = Context()
        ctx_b = Context()
        captured: list[Token] = []

        def inside_a():
            captured.append(var.set("a_value"))

        ctx_a.run(inside_a)
        token = captured[0]

        def inside_b():
            var.reset(token)

        # Act & assert
        with pytest.raises(ValueError, match="different wool.Context"):
            ctx_b.run(inside_b)

    def test_reset_with_reconstituted_token_when_chain_id_differs(self):
        """Test reset raises ValueError for a reconstituted Token whose
        originating chain id does not match the current Context.

        Given:
            A ContextVar, a Token minted in Context A and pickled,
            the original Token then released and garbage-collected
            so the process-wide token registry's weak entry drops,
            and a fresh scoped_context(B) block whose id differs
            from A
        When:
            The pickled bytes are loaded (producing a reconstituted
            Token with _context=None and _context_id=A) and
            var.reset(restored) is called under Context B
        Then:
            ValueError is raised with a message naming the different
            wool.Context — the UUID fallback check on the
            reconstituted branch fires because object-identity
            comparison isn't available
        """
        # Arrange
        var = ContextVar("reconstituted_chain_mismatch", default="d")
        pickled: bytes

        def mint_and_release() -> bytes:
            with scoped_context():
                token = var.set("x")
                return cloudpickle.dumps(token)

        pickled = mint_and_release()
        gc.collect()

        # Act & assert
        with scoped_context():
            restored = cloudpickle.loads(pickled)
            with pytest.raises(ValueError, match="different wool.Context"):
                var.reset(restored)

    def test_reset_restores_unset_in_different_context_scope(self):
        """Test reset restores unset state when old_value was MISSING.

        Given:
            A previously-unset ContextVar whose set() Token captured
            MISSING as the old_value, then a different wool.Context
            scope entered via Context.run
        When:
            reset(token) is invoked in the scoped Context
        Then:
            The var should revert to unset; get(fallback) returns the
            supplied fallback
        """
        # Arrange
        var = ContextVar("reset_unset_fallback")
        token = var.set("briefly")
        seeded = contextvars.copy_context()

        def body():
            var.set("nested")
            var.reset(token)
            return var.get("<fallback>")

        # Act
        result = seeded.run(body)

        # Assert
        assert result == "<fallback>"

    def test_pickle_roundtrip_when_var_registered(self):
        """Test cloudpickle roundtrips a ContextVar to the same registered instance.

        Given:
            A ContextVar registered in the process-wide registry
        When:
            It is pickled and unpickled via dumps / loads
        Then:
            The unpickled instance should be the same object as the original
        """
        # Arrange
        var = ContextVar("shipped")

        # Act
        restored = loads(dumps(var))

        # Assert
        assert restored is var

    def test_pickle_roundtrip_when_key_unregistered_on_receiver(self):
        """Test loads() creates a stub when the var key is not registered.

        Given:
            Pickle bytes of a ContextVar whose registry slot has been
            reclaimed via GC (the var instance was constructed inside
            a nested Context.run block, pickled with an embedded value
            while the strong ref was alive, and released on block
            exit so the WeakValueDictionary entry can drop)
        When:
            loads(pickled) is called inside a fresh Context scope
        Then:
            The restored var's key matches the original, and get()
            returns the embedded value that was set at pickle time
            — confirming the stub path was taken and the embedded
            payload was applied to the receiver's Context
        """
        # Arrange — pickle inside a nested Context.run so the
        # ephemeral var's strong ref in that Context's data dict
        # drops with the scope, letting the WeakValueDictionary slot
        # clear on gc.collect().
        captured: list[tuple[bytes, str]] = []

        def pickle_ephemeral():
            ephemeral = ContextVar(
                "stub_unknown",
                namespace="test_stub_create",
                default="d",
            )
            ephemeral.set("wire_value")
            captured.append((dumps(ephemeral), ephemeral.key))

        Context().run(pickle_ephemeral)
        gc.collect()

        pickled, original_key = captured[0]

        observed: list[tuple[str, object]] = []

        def in_fresh():
            restored = loads(pickled)
            observed.append((restored.key, restored.get("missing")))

        # Act
        Context().run(in_fresh)

        # Assert
        assert len(observed) == 1
        key, value = observed[0]
        assert key == original_key
        assert value == "wire_value"

    def test_pickle_roundtrip_embeds_current_value(self):
        """Test pickling a ContextVar captures its current value for the receiver.

        Given:
            A ContextVar set to a specific value in the current context
        When:
            The var is pickled and unpickled inside a fresh wool.Context
        Then:
            The receiver's var.get() returns the value that was set at
            pickle time — the reducer-override embedded it
        """
        # Arrange
        var = ContextVar("pickle_with_value", default="default_value")
        var.set("pickled_value")
        pickled = dumps(var)

        # Act
        observed: list[object] = []

        def in_fresh():
            restored = loads(pickled)
            observed.append(restored.get())

        Context().run(in_fresh)

        # Assert
        assert observed == ["pickled_value"]

    def test_pickle_roundtrip_when_var_unset(self):
        """Test pickling a never-set ContextVar doesn't apply a value on receive.

        Given:
            A ContextVar that has never been set in the current context
        When:
            The var is pickled and unpickled inside a fresh wool.Context
        Then:
            The receiver's var.get() returns the class-level default
            and the receiver's Context does not contain the var
            (no value was embedded in the pickle)
        """
        # Arrange
        var = ContextVar("pickle_no_value", default="default_value")
        pickled = dumps(var)

        # Act
        observed_get: list[object] = []
        observed_ctx: list[Context] = []

        def in_fresh():
            restored = loads(pickled)
            observed_get.append(restored.get())
            observed_ctx.append(current_context())

        Context().run(in_fresh)

        # Assert
        assert observed_get == ["default_value"]
        assert var not in observed_ctx[0]

    def test_later_declaration_after_preloaded_var(self):
        """Test a later ContextVar declaration promotes a pre-existing stub.

        Given:
            A stub produced by unpickling an unknown-key pickle —
            the registry holds a stub after loads()
        When:
            ContextVar(name, namespace=ns, default=<v>) is constructed
            with the same (namespace, name)
        Then:
            The declaration returns the same instance as the restored
            stub (no ContextVarCollision raised), and the declaration's
            default value is observed via var.get() in a fresh Context
            distinct from the stub's wire-value receiver Context
        """
        # Arrange — pickle inside a nested Context.run, let gc clear
        # the slot, then unpickle to create a stub.
        captured: list[bytes] = []

        def pickle_ephemeral():
            ephemeral = ContextVar(
                "stub_promoted",
                namespace="test_stub_promote",
                default="d",
            )
            ephemeral.set("wire_value")
            captured.append(dumps(ephemeral))

        Context().run(pickle_ephemeral)
        gc.collect()

        pickled = captured[0]

        # Hold a strong reference to the receiver Context so it
        # outlives the pickle load, ensuring the later
        # ContextVar(...) declaration runs against the registry
        # state the load produced rather than landing on state
        # already reclaimed by GC.
        receiver = Context()
        stub_capture: list[ContextVar] = []

        def in_receiver():
            stub_capture.append(loads(pickled))

        receiver.run(in_receiver)
        restored = stub_capture[0]

        # Act — authoritative declaration with a new default.
        promoted = ContextVar(
            "stub_promoted",
            namespace="test_stub_promote",
            default="auth_default",
        )

        # Assert — same instance (stub was promoted, not replaced).
        assert promoted is restored

        # And the promoted default is observed in a fresh Context
        # (distinct from the receiver Context that holds wire_value).
        observed_default: list[object] = []

        def in_fresh():
            observed_default.append(promoted.get())

        Context().run(in_fresh)

        assert observed_default == ["auth_default"]

    def test_repr_includes_key(self):
        """Test ContextVar repr includes the full key.

        Given:
            A ContextVar with a name
        When:
            repr() is called on it
        Then:
            The repr should include 'namespace:name'
        """
        # Arrange
        var = ContextVar("repr_cv")

        # Act
        text = repr(var)

        # Assert
        assert f"'{var.key}'" in text


class TestToken:
    def test_pickle_roundtrip_with_var_reference(self):
        """Test Token pickle roundtrip carries its owning ContextVar by key.

        Given:
            A ContextVar and a Token produced by set()
        When:
            The Token is pickled and unpickled
        Then:
            The restored token should reference the same ContextVar instance
        """
        # Arrange
        var = ContextVar("tokened")
        token = var.set("x")

        # Act
        restored = loads(dumps(token))

        # Assert
        assert restored.var is var
        assert restored.old_value is Token.MISSING

    def test_pickle_roundtrip_in_same_process(self):
        """Test same-process pickle of a Token returns the same instance.

        Given:
            A live Token minted via ContextVar.set — strongly
            referenced so its entry in the process-wide token
            registry stays alive
        When:
            The Token is pickled and unpickled in the same process
        Then:
            The restored token should be the same Python object as
            the original — the registry lookup in Token._reconstitute
            resolves the id back to the live instance so mutations
            to ``_used`` stay visible across all references
        """
        # Arrange
        var = ContextVar("pickle_identity")
        token = var.set("x")

        # Act
        restored = loads(dumps(token))

        # Assert
        assert restored is token

    def test_pickle_roundtrip_with_used_flag(self):
        """Test Token.__reduce__ serializes the _used flag.

        Given:
            A Token minted and then consumed via ContextVar.reset,
            pickled after consumption
        When:
            The pickled bytes are loaded in a context where the
            original Token is no longer reachable (registry miss
            forces _reconstitute to build a fresh stub)
        Then:
            The restored stub should have used=True — the flag
            rides the pickle tuple so a cross-process copy cannot
            silently attempt reset
        """
        # Arrange
        var = ContextVar("pickle_used_flag")
        token = var.set("x")
        var.reset(token)
        pickled = dumps(token)
        original_id_in_memory = id(token)
        del token
        gc.collect()

        # Act
        restored = loads(pickled)

        # Assert
        assert restored.used is True
        assert id(restored) != original_id_in_memory

    def test_pickle_roundtrip_in_active_receiver_context(self):
        """Test pickling a Token does not mutate the receiver's
        :class:`Context` via an embedded var value.

        Given:
            A wool.ContextVar bound to value ``"A"`` in one Context,
            with a Token pickled while that binding is active. The
            receiver later enters a different Context where the same
            var has been bound to ``"B"``.
        When:
            The pickled Token bytes are loaded under the receiver's
            Context
        Then:
            The receiver's Context should still report ``"B"`` for
            the var — a Token is a reset receipt, not a value-
            bearing wire payload, so its pickle round-trip must not
            transitively propagate the originating Context's binding
            through the owning ContextVar's reduce path
        """
        # Arrange
        var = ContextVar(f"r2c5_token_no_value_leak_{uuid.uuid4().hex}")
        token = var.set("A")
        pickled = dumps(token)
        var.reset(token)
        observed: list[str] = []

        # Act
        with scoped_context():
            var.set("B")
            loads(pickled)
            observed.append(var.get())

        # Assert
        assert observed == ["B"]

    def test_set_reset_loop_token_lifecycle(self):
        """Test a tight set/reset loop does not accumulate per-iteration state.

        Given:
            A ContextVar in a fresh Context scope and a weakref to a
            sampled iteration's Token
        When:
            set(value) followed by reset(token) runs many times
            with no lingering strong reference to any iteration's
            Token, followed by gc.collect()
        Then:
            The sampled Token's weakref should resolve to None —
            each iteration's Token becomes unreachable once the
            loop's local binding is overwritten, so no lingering
            per-iteration state keeps it alive
        """
        # Arrange
        var = ContextVar("loop_no_leak")
        sampled_ref: weakref.ref[Token] | None = None

        # Act
        for i in range(200):
            token = var.set("x")
            if i == 100:
                sampled_ref = weakref.ref(token)
            var.reset(token)
            del token
        gc.collect()

        # Assert
        assert sampled_ref is not None
        assert sampled_ref() is None

    def test_repr_includes_var_key(self):
        """Test Token repr includes the owning var's key.

        Given:
            A Token produced by set() on a ContextVar
        When:
            repr() is called on it
        Then:
            The repr should include the var's full key
        """
        # Arrange
        var = ContextVar("repr_token_var")
        token = var.set("x")

        # Act
        text = repr(token)

        # Assert
        assert var.key in text

    def test_missing_repr(self):
        """Test Token.MISSING repr labels the sentinel identifiably.

        Given:
            Token.MISSING — the module-wide unset sentinel
        When:
            repr(Token.MISSING) is evaluated
        Then:
            It returns "<Token.MISSING>" — the sentinel is labeled
            identifiably in diagnostics
        """
        # Act & assert
        assert repr(Token.MISSING) == "<Token.MISSING>"

    def test_missing_in_boolean_context(self):
        """Test Token.MISSING is falsy-by-design.

        Given:
            Token.MISSING
        When:
            bool(Token.MISSING) is evaluated
        Then:
            It returns False — the sentinel is distinct from
            user-value None/0 but truth-tests as absence
        """
        # Act & assert
        assert bool(Token.MISSING) is False

    def test_missing_pickle_roundtrip_singleton_identity(self):
        """Test pickling and unpickling Token.MISSING returns the same
        singleton instance.

        Given:
            Token.MISSING — a singleton-by-construction sentinel
            whose ``__new__`` caches the lone instance and whose
            ``__reduce__`` rebuilds via the same constructor
        When:
            Token.MISSING is pickled with cloudpickle and the bytes
            are loaded back
        Then:
            The reloaded value is identical to the original —
            ``loaded is Token.MISSING`` — so callers comparing
            ``token.old_value is Token.MISSING`` after a wire round-
            trip still hit the identity check.
        """
        # Arrange
        original = Token.MISSING

        # Act
        pickled = dumps(original)
        loaded = loads(pickled)

        # Assert
        assert loaded is original
        assert loaded is Token.MISSING

    def test_used_is_false_before_reset_and_true_after(self):
        """Test Token.used flips from False to True when the owning var is reset.

        Given:
            A ContextVar and a Token produced by var.set()
        When:
            var.reset(token) is called
        Then:
            Token.used should be False before the reset and True
            after — single-process sanity for the lifecycle flag
        """
        # Arrange
        var = ContextVar("used_flag", default="d")
        token = var.set("x")

        # Act & assert
        assert token.used is False
        var.reset(token)
        assert token.used is True


class TestContext:
    def test___new___with_direct_instantiation(self):
        """Test Context() constructs an empty Context with a fresh id.

        Given:
            The Context class
        When:
            It is instantiated directly
        Then:
            The result should have a fresh id and no captured vars
        """
        # Act
        ctx = Context()

        # Assert
        assert ctx.id is not None
        assert len(ctx) == 0

    def test___bool___is_false_for_fresh_context(self):
        """Test bool(Context()) is False when no state has been captured.

        Given:
            A freshly constructed empty Context
        When:
            bool() is invoked on it
        Then:
            The result should be False so callers can use
            ``if not ctx:`` as a fast-path gate
        """
        # Act
        ctx = Context()

        # Assert
        assert bool(ctx) is False

    def test___bool___when_var_is_set(self):
        """Test bool(ctx) is True once a ContextVar has been set in it.

        Given:
            A Context with a var bound via ContextVar.set
        When:
            bool() is invoked on it
        Then:
            The result should be True
        """
        # Arrange
        var = ContextVar("bool_var_set", default="initial")
        var.set("value")
        ctx = current_context()

        # Act
        result = bool(ctx)

        # Assert
        assert result is True

    def test___bool___is_true_when_incoming_used_ids_are_present(self):
        """Test bool(ctx) is True when _data is empty but incoming-used-ids exist.

        Given:
            A wire protocol.Context with no var bindings but a
            non-empty consumed_tokens list (modeling a back-prop
            response that only carries used-token state)
        When:
            Context.from_protobuf reconstructs it and bool() is
            invoked on the result
        Then:
            The result should be True — the ids must be applied on
            merge so the corresponding live tokens flip their _used
            flags
        """
        # Arrange
        from wool import protocol

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.consumed_tokens.append(uuid.uuid4().hex)

        # Act
        reconstructed = Context.from_protobuf(pb)

        # Assert
        assert len(reconstructed) == 0
        assert bool(reconstructed) is True

    def test___bool___when_used_token_ids_are_present(self):
        """Test bool(ctx) is True when _data is empty but the
        used-token-id set populated by ``ContextVar.reset`` is
        non-empty.

        Given:
            A Context that ran a set+reset cycle on a ContextVar —
            the var binding cleared back to MISSING but the consumed
            token id is recorded in ``_used_token_ids`` for outbound
            wire emission
        When:
            bool() is invoked on the Context
        Then:
            The result should be True — the Context still carries
            wire-shippable state via ``Context.to_protobuf``'s
            ``consumed_tokens`` field, so the bool surface aligns
            with what the wire emission produces rather than
            silently reporting empty
        """
        # Arrange
        var = ContextVar(f"r2c6_used_token_id_truthy_{uuid.uuid4().hex}")
        ctx = Context()

        def consume():
            t = var.set("x")
            var.reset(t)

        ctx.run(consume)

        # Act
        result = bool(ctx)

        # Assert
        assert len(ctx) == 0
        assert result is True

    def test_update_with_live_token_when_wire_carries_id(self):
        """Test Context.update flips a live unused Token's used flag
        when the incoming wire Context lists its id.

        Given:
            A live Token whose used flag is False, and a temp
            Context reconstructed from a wire message whose
            consumed_tokens list contains that Token's id
        When:
            current_context().update(temp) is called
        Then:
            Token.used on the live Token flips to True — the merge
            resolves the incoming id through the process-wide token
            registry and applies cross-process consumption to the
            local instance
        """
        # Arrange
        from wool import protocol

        var = ContextVar("update_flip_used", default="d")
        token = var.set("x")
        assert token.used is False

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.consumed_tokens.append(token.id.hex)
        incoming = Context.from_protobuf(pb)

        # Act
        current_context().update(incoming)

        # Assert
        assert token.used is True

    def test_update_with_incoming_id_and_no_live_token(self):
        """Test Context.update ignores incoming ids with no live Token.

        Given:
            A current Context and a temp Context reconstructed from
            a wire message whose consumed_tokens list contains a
            UUID that matches no live Token in the process
        When:
            current_context().update(temp) is called
        Then:
            The merge completes without raising — unregistered
            incoming ids are silently dropped because no peer Token
            object exists to flip
        """
        # Arrange
        from wool import protocol

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.consumed_tokens.append(uuid.uuid4().hex)
        incoming = Context.from_protobuf(pb)

        # Act & assert
        current_context().update(incoming)

    def test_run_seeds_vars_and_scopes_mutations(self):
        """Test Context.run seeds vars in a fresh stdlib Context and scopes mutations.

        Given:
            A Context captured with an initial var value
        When:
            Context.run() runs a function that mutates the var
        Then:
            Mutations should be visible inside the run and captured on exit
        """
        # Arrange
        var = ContextVar("run_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        def body():
            assert var.get() == "seeded"
            var.set("mutated")
            return var.get()

        # Act
        result = ctx.run(body)

        # Assert
        assert result == "mutated"
        assert ctx[var] == "mutated"

    def test_run_binds_context_for_sync_callers(self):
        """Test Context.run makes self.id the active Context id inside fn.

        Given:
            A Context constructed directly (sync caller, no asyncio task)
        When:
            Context.run invokes a function that reads current_context().id
        Then:
            The reported context id equals the Context's own id, not the
            process-default id
        """
        # Arrange
        ctx = Context()

        # Act
        observed = ctx.run(lambda: current_context().id)

        # Assert
        assert observed == ctx.id

    def test_run_snapshot_with_unset_or_reset_vars(self):
        """Test Context.run's snapshot excludes vars that look unset at exit.

        Given:
            One var never set inside the run, one var that is set and
            then reset inside the run
        When:
            Context.run() returns and captures the post-run snapshot
        Then:
            Neither var should appear in the Context's captured vars
        """
        # Arrange
        untouched = ContextVar("untouched", default="x")
        set_then_reset = ContextVar("set_then_reset")
        ctx = Context()

        def body():
            token = set_then_reset.set("temp")
            set_then_reset.reset(token)

        # Act
        ctx.run(body)

        # Assert
        assert untouched not in ctx
        assert set_then_reset not in ctx

    def test_run_with_re_entry_on_same_task(self):
        """Test Context.run rejects a nested run() call on the same task.

        Given:
            A Context already executing a synchronous run() body
        When:
            A second run() is attempted from inside the body, on
            the same thread/task
        Then:
            The single-task guard rejects the inner call —
            re-entry from the owning execution scope is treated the
            same as concurrent entry from another scope. Cross-thread
            concurrency is exercised separately by
            ``test_run_async_with_concurrent_entry``.
        """
        # Arrange
        ctx = Context()

        def outer():
            with pytest.raises(RuntimeError):
                ctx.run(lambda: None)

        # Act & assert
        ctx.run(outer)

    @pytest.mark.asyncio
    async def test_run_async_attaches_context_across_await(self):
        """Test Context.run_async keeps the Context attached across the
        coroutine's suspension points.

        Given:
            A Context populated with a ContextVar binding and an
            async function that suspends via ``await asyncio.sleep(0)``
            before reading the var
        When:
            ctx.run_async(fn) is awaited
        Then:
            The post-suspension read should observe the Context's
            value — the attach scope spans the entire coroutine
            body, not just the synchronous frame that constructed
            the coroutine
        """
        # Arrange
        var = ContextVar("run_async_attach", default="default")
        ctx = Context()
        ctx.run(lambda: var.set("inside"))
        observed: list[str] = []

        async def read_after_suspend():
            await asyncio.sleep(0)
            observed.append(var.get())

        # Act
        await ctx.run_async(read_after_suspend)

        # Assert
        assert observed == ["inside"]

    @pytest.mark.asyncio
    async def test_run_async_with_sync_fn(self):
        """Test Context.run_async accepts a sync callable and returns its value.

        Given:
            A Context and a synchronous callable returning a value
        When:
            ctx.run_async(fn) is awaited
        Then:
            The awaited result equals the callable's return value
            — run_async unifies sync and async paths so callers do
            not branch on hook shape
        """
        # Arrange
        ctx = Context()

        def sync_fn():
            return 42

        # Act
        result = await ctx.run_async(sync_fn)

        # Assert
        assert result == 42

    @pytest.mark.asyncio
    async def test_run_async_with_concurrent_entry(self):
        """Test Context.run_async raises when another task is already
        running inside the same Context.

        Given:
            A Context with one task suspended inside ``run_async``
            (holding the single-task guard across an await)
        When:
            A concurrent task attempts run_async on the same Context
        Then:
            It should raise RuntimeError — the single-task
            invariant holds across the await window, not just the
            synchronous portion
        """
        # Arrange
        ctx = Context()
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        outcomes: list[str] = []

        async def first():
            first_entered.set()
            await release_first.wait()

        async def second():
            try:
                await ctx.run_async(lambda: None)
            except RuntimeError:
                outcomes.append("second-rejected")

        first_task = asyncio.create_task(ctx.run_async(first))
        await first_entered.wait()

        # Act
        await second()
        release_first.set()
        await first_task

        # Assert
        assert outcomes == ["second-rejected"]

    def test_copy_with_populated_source(self):
        """Test Context.copy returns a sibling Context with the same
        var bindings but a fresh logical-chain id.

        Given:
            A Context populated with one or more var bindings via
            ``Context.run``
        When:
            ``source.copy()`` is invoked on the populated Context,
            and a second ``source.copy()`` is invoked alongside
        Then:
            Each copy holds the same var bindings as the source,
            each copy's id differs from the source's id, and the
            two copies' ids differ from each other — mirrors
            ``contextvars.Context.copy`` semantics with wool's
            chain-id contract that copies are new chains in the
            tree, not aliases of the source
        """
        # Arrange
        var = ContextVar(f"copy_source_{uuid.uuid4().hex}")
        source = Context()
        source.run(lambda: var.set("seed-value"))

        # Act
        sibling_a = source.copy()
        sibling_b = source.copy()

        # Assert
        assert sibling_a.id != source.id
        assert sibling_b.id != source.id
        assert sibling_a.id != sibling_b.id
        assert sibling_a[var] == "seed-value"
        assert sibling_b[var] == "seed-value"

    def test_iter_yields_captured_vars(self):
        """Test Context iterates over captured ContextVar instances.

        Given:
            A Context with multiple captured vars
        When:
            It is iterated
        Then:
            The iterator should yield each captured var
        """
        # Arrange
        a = ContextVar("iter_a", default=0)
        b = ContextVar("iter_b", default=0)
        a.set(1)
        b.set(2)

        # Act
        ctx = current_context()

        # Assert
        assert set(iter(ctx)) == {a, b}

    def test_getitem_with_captured_value(self):
        """Test Context[var] returns the captured value.

        Given:
            A Context with a captured var
        When:
            The Context is indexed by the var
        Then:
            The captured value should be returned
        """
        # Arrange
        var = ContextVar("get_item", default=0)
        var.set(1)
        ctx = current_context()

        # Act & assert
        assert ctx[var] == 1

    def test_contains_reports_membership(self):
        """Test `var in ctx` reports whether the var was captured.

        Given:
            A Context with one captured var and one uncaptured var
        When:
            Membership is tested for each
        Then:
            The captured var should be present and the uncaptured absent
        """
        # Arrange
        in_ctx = ContextVar("present_var", default=0)
        out_ctx = ContextVar("absent_var", default=0)
        in_ctx.set(1)

        # Act
        ctx = current_context()

        # Assert
        assert in_ctx in ctx
        assert out_ctx not in ctx

    def test_len_with_captured_vars(self):
        """Test len(ctx) returns the number of captured vars.

        Given:
            A Context with two captured vars
        When:
            len() is called on the Context
        Then:
            It should return 2
        """
        # Arrange
        a = ContextVar("len_a", default=0)
        b = ContextVar("len_b", default=0)
        a.set(1)
        b.set(2)

        # Act
        ctx = current_context()

        # Assert
        assert len(ctx) == 2

    def test_keys_values_items_expose_captured_pairs(self):
        """Test Context keys/values/items expose the captured mapping.

        Given:
            A Context with two captured vars
        When:
            keys(), values(), items() are called
        Then:
            Each accessor should return the expected captured pairs
        """
        # Arrange
        a = ContextVar("kvitems_a", default="")
        b = ContextVar("kvitems_b", default="")
        a.set("x")
        b.set("y")

        # Act
        ctx = current_context()

        # Assert
        assert set(ctx.keys()) == {a, b}
        assert set(ctx.values()) == {"x", "y"}
        assert dict(ctx.items()) == {a: "x", b: "y"}

    def test_get_with_set_value_or_default(self):
        """Test Context.get returns the set value or the supplied default.

        Given:
            A Context with one var set and another var that was never
            set in this Context
        When:
            get(var) and get(var, default) are called
        Then:
            The set var returns its value; the unset var returns the
            supplied default (or None if no default is given)
        """
        # Arrange
        set_var = ContextVar("get_set", default="class-default")
        unset_var = ContextVar("get_unset", default="class-default")
        set_var.set("value")
        ctx = current_context()

        # Act & assert
        assert ctx.get(set_var) == "value"
        assert ctx.get(set_var, "fallback") == "value"
        assert ctx.get(unset_var) is None
        assert ctx.get(unset_var, "fallback") == "fallback"

    def test_repr_includes_id_and_var_count(self):
        """Test Context repr mentions id and number of vars.

        Given:
            A Context with one captured var
        When:
            repr() is called on it
        Then:
            The repr should contain "id=" and "vars=1"
        """
        # Arrange
        var = ContextVar("repr_var", default=0)
        var.set(1)
        ctx = current_context()

        # Act
        text = repr(ctx)

        # Assert
        assert "id=" in text
        assert "vars=1" in text

    def test___reduce___under_pickle_copy_and_deepcopy(self):
        """Test wool.Context refuses pickle, copy.copy, and copy.deepcopy.

        Given:
            A live wool.Context
        When:
            pickle.dumps, copy.copy, and copy.deepcopy are each
            invoked on it
        Then:
            All three raise TypeError, mirroring the stdlib
            contextvars.Context policy — callers must use
            Context.copy() explicitly for in-process duplication.
            Pickle would produce a snapshot disconnected from any
            live state; matching the stdlib refusal closes that
            footgun.
        """
        # Arrange
        import copy as _copy
        import pickle

        var = ContextVar("ctx_unpicklable", default="zero")
        var.set("one")
        ctx = current_context()

        # Act & assert
        with pytest.raises(TypeError, match="wool.Context"):
            pickle.dumps(ctx)
        with pytest.raises(TypeError, match="wool.Context"):
            _copy.copy(ctx)
        with pytest.raises(TypeError, match="wool.Context"):
            _copy.deepcopy(ctx)

    def test_to_protobuf_with_unpicklable_value(self):
        """Test Context.to_protobuf raises TypeError naming the offending var.

        Given:
            A ContextVar set to an unpicklable value (a local generator
            function object)
        When:
            current_context().to_protobuf() is called to snapshot the
            current vars
        Then:
            TypeError is raised with a message naming the offending var
            key, per the public serialization contract that
            non-serializable values surface a TypeError at dispatch
            time.
        """
        # Arrange
        var = ContextVar("ctx001_unpicklable")

        def _local_gen():
            yield 1

        var.set(_local_gen())

        # Act & assert
        with pytest.raises(TypeError, match=var.key):
            current_context().to_protobuf()

    def test_update_with_empty_context_is_noop(self):
        """Test update applied with an empty peer leaves state unchanged.

        Given:
            A Context with a var set and an empty peer Context
        When:
            current.update(empty) is called
        Then:
            The current context is unchanged and no exception is
            raised.
        """
        # Arrange
        from wool import protocol

        var = ContextVar("ctx002_seed", default="default")
        var.set("before")
        before = current_context()
        empty = Context.from_protobuf(protocol.Context())

        # Act
        before.update(empty)

        # Assert
        after = current_context()
        assert after[var] == before[var]
        assert set(after.keys()) == set(before.keys())

    def test_from_protobuf_with_unknown_keys_alongside_known_ones(self):
        """Test Context.from_protobuf stubs unknown keys and applies their
        values, while still deserializing known keys as normal.

        Given:
            A wire-form protocol.Context carrying a mix of keys — one
            registered on this process, one not
        When:
            Context.from_protobuf is invoked with the payload
        Then:
            The registered key is deserialized as before, and the
            unregistered key results in a stub entry so the receiver
            can observe the propagated value when the var is later
            declared (rolling-deploy / lazy-import scenario).
        """
        # Arrange
        from wool import protocol

        known_var = ContextVar("ctx003_known", default="initial")
        unknown_ns = f"ctx003_unknown_{uuid.uuid4().hex}"
        unknown_key = f"{unknown_ns}:missing"
        pb = protocol.Context(
            vars={
                unknown_key: dumps("propagated"),
                known_var.key: dumps("applied"),
            }
        )

        # Act
        reconstructed = Context.from_protobuf(pb)
        current_context().update(reconstructed)

        # Assert
        assert reconstructed[known_var] == "applied"
        late_declared: ContextVar[str] = ContextVar("missing", namespace=unknown_ns)
        assert late_declared.get() == "propagated"

    def test_from_protobuf_with_unregistered_key_then_later_var_declaration(
        self,
    ):
        """Test wire ingress of an unregistered var matches the pickle-path
        stub-promotion semantics.

        Given:
            A wire protocol.Context carrying a var key that is not yet
            registered on this process, and the corresponding
            wool.ContextVar declaration arrives later (lazy-import on
            the receiver)
        When:
            Context.from_protobuf reconstructs the payload,
            current_context().update merges it, and the user then
            declares the ContextVar under the same key
        Then:
            ContextVar.get should return the wire-propagated value —
            the wire-ingress path creates and pins a stub the same way
            the pickled-ContextVar-instance path does, so lazy-import
            receivers converge after one dispatch rather than needing
            a second one that carries a ContextVar instance in-args.
        """
        # Arrange
        from wool import protocol

        unique_ns = f"wire_stub_{uuid.uuid4().hex}"
        key = f"{unique_ns}:tenant_id"
        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars[key] = dumps("acme-corp")

        incoming = Context.from_protobuf(pb)
        current_context().update(incoming)

        # Act
        var: ContextVar[str] = ContextVar("tenant_id", namespace=unique_ns)

        # Assert
        assert var.get() == "acme-corp"

    def test_from_protobuf_with_corrupt_value(self):
        """Test Context.from_protobuf raises ValueError naming a
        corrupt key.

        Given:
            A registered wool.ContextVar and a wire-form
            protocol.Context containing that key mapped to bytes that
            are not a valid pickle stream
        When:
            Context.from_protobuf is invoked with the payload
        Then:
            It raises ValueError and the message includes the
            offending key so operators can identify which wire entry
            is malformed.
        """
        # Arrange
        from wool import protocol

        var = ContextVar("ctx003_corrupt", default="initial")
        pb = protocol.Context(vars={var.key: b"\x00not a valid pickle stream\x00"})

        # Act & assert
        with pytest.raises(ValueError, match=var.key):
            Context.from_protobuf(pb)

    def test_from_protobuf_with_malformed_consumed_token_ids(
        self,
    ):
        """Test Context.from_protobuf tolerates a single malformed
        consumed-token id without aborting the whole frame decode.

        Given:
            A wire-form protocol.Context carrying a valid var
            binding, a valid consumed-token hex id, and a malformed
            consumed-token hex id (not a UUID)
        When:
            Context.from_protobuf is invoked with the payload
        Then:
            The valid var binding is applied, the valid consumed-token
            id lands in the reconstructed Context's incoming buffer,
            the malformed id is skipped with a RuntimeWarning naming
            it, and no ValueError propagates — matching the
            per-var log-and-skip policy already in place for var
            values.
        """
        # Arrange
        from wool import protocol

        var = ContextVar("ctx_c6_partial", default="d")
        valid_id = uuid.uuid4()
        pb = protocol.Context(
            id=uuid.uuid4().hex,
            vars={var.key: dumps("applied")},
        )
        pb.consumed_tokens.append(valid_id.hex)
        pb.consumed_tokens.append("not-a-uuid")

        # Act
        with pytest.warns(RuntimeWarning, match="not-a-uuid"):
            reconstructed = Context.from_protobuf(pb)

        # Assert
        assert reconstructed[var] == "applied"
        current_context().update(reconstructed)
        emitted = current_context().to_protobuf()
        assert valid_id.hex in set(emitted.consumed_tokens)

    def test_from_protobuf_with_stub_pinning(self):
        """Test Context.from_protobuf attaches resolved stubs to the
        Context it constructs and returns, not to the caller's
        currently-active Context.

        Given:
            A wire-form protocol.Context carrying an unregistered
            namespaced key, decoded from inside an outer
            scoped_context block. The returned Context is then
            dropped while the outer Context is still in scope, so
            only the pin anchor's keep-alive can preserve the stub
            once the outer block subsequently exits
        When:
            The returned Context is dropped, the outer scope exits,
            and gc.collect runs
        Then:
            The stub should NOT be discoverable in the process-wide
            var registry — the pin attribution lives on the
            returned Context (now gone), so the stub is reclaimed.
            If the pin had attached to the outer Context the stub
            would have outlived the returned Context (an attribution
            inversion).
        """
        # Arrange
        from wool import protocol
        from wool.runtime.context import base as ctx_base

        key = f"r2c3_pin_attribution:{uuid.uuid4().hex}"
        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars[key] = cloudpickle.dumps("propagated")

        # Act
        with scoped_context():
            incoming = Context.from_protobuf(pb)
            del incoming
            gc.collect()
            # Window: returned Context dropped, outer scope still
            # active. With the pin on incoming this releases the
            # stub immediately; with the pin on outer the stub
            # would survive until outer also dies.
            in_registry_after_incoming_dies = key in ctx_base.var_registry

        # Assert
        assert in_registry_after_incoming_dies is False

    @pytest.mark.asyncio
    async def test_from_protobuf_in_caller_task(self):
        """Test Context.from_protobuf does not lazy-register a Context
        on the calling task as a side effect of decoding a wire frame.

        Given:
            An asyncio task whose scope has no Context registered —
            a fresh ``asyncio.create_task`` child built without wool's
            task factory installed
        When:
            Context.from_protobuf is called from inside that task
        Then:
            ``peek()`` from inside the task should still return None
            after the call — decoding a wire frame must not
            materialize a Context on the decoding task's scope
        """
        # Arrange
        from wool import protocol
        from wool.runtime.context import peek

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars[f"r2c3_no_side_effect:{uuid.uuid4().hex}"] = cloudpickle.dumps("v")
        observed: list[Context | None] = []

        async def body():
            Context.from_protobuf(pb)
            observed.append(peek())

        # Act
        await asyncio.create_task(body())

        # Assert
        assert observed == [None]

    def test_pickle_roundtrip_with_embedded_value_on_receiver(self):
        """Test reconstructing a var writes its embedded value into the
        receiver's Context.

        Given:
            A wool.ContextVar set to value ``v`` and pickled via
            dumps (which embeds the current value in the reduce
            tuple)
        When:
            The pickled bytes are unpickled inside a fresh
            wool.Context scope and current_context() is captured
        Then:
            The reconstructed var appears in the captured Context
            (``var in ctx``) with the embedded value (``ctx[var]``)
            — confirming reconstruct applies the embedded value to
            the receiver's Context.
        """
        # Arrange — pickle while the var holds "wire_value", then
        # reset so the receiver's Context no longer has the value.
        # Unpickling forces reconstruct to apply the embedded value
        # into the receiver's Context.
        var = ContextVar("ctx004_roundtrip", default="d")
        token = var.set("wire_value")
        pickled = dumps(var)
        var.reset(token)

        observed: list[Context] = []

        def in_fresh():
            restored = loads(pickled)
            observed.append(current_context())
            assert restored.get() == "wire_value"

        # Act
        Context().run(in_fresh)

        # Assert
        assert len(observed) == 1
        assert var in observed[0]
        assert observed[0][var] == "wire_value"

    def test_update_merges_consumed_tokens(self):
        """Test Context.update merges consumed-token state from the source.

        Given:
            Two distinct Contexts — a secondary that has consumed
            a Token via ContextVar.reset, and a primary that hasn't
        When:
            primary.update(secondary) is called
        Then:
            A pickle-alias of the consumed Token should report
            Token.used as True when inspected with the primary as
            the active Context — back-propagation's core carry
            mechanism for cross-process consumption
        """
        # Arrange
        var = ContextVar("merge_consumed", default="d")
        shared_id = uuid.uuid4()
        tokens: list[Token] = []
        secondaries: list[Context] = []

        with scoped_context(shared_id):
            t = var.set("x")
            var.reset(t)
            tokens.append(t)
            secondaries.append(current_context())
        secondary = secondaries[0]
        alias = loads(dumps(tokens[0]))

        primary = Context()

        # Act
        primary.update(secondary)

        # Assert
        primary_sees: list[bool] = []
        primary.run(lambda: primary_sees.append(alias.used))
        assert primary_sees == [True]

    def test_to_protobuf_with_locally_reset_token(self):
        """Test Context.to_protobuf emits consumed-token ids scoped to
        this Context's logical chain, excluding tokens reset under a
        different chain.

        Given:
            Two Contexts A and B with distinct ids, a Token minted
            and reset under A, and a Token minted and reset under B
        When:
            A.to_protobuf() is called
        Then:
            The resulting ``consumed_tokens`` list contains A's
            token id but not B's — the per-lineage scoping of
            wire emission holds regardless of whether it is derived
            from a global scan or from per-Context bookkeeping
        """
        # Arrange
        var_a = ContextVar("c4_pin_scope_a", default="d")
        var_b = ContextVar("c4_pin_scope_b", default="d")

        a_tokens: list[Token] = []
        b_tokens: list[Token] = []

        def consume_in_a() -> None:
            a_tokens.append(var_a.set("ax"))
            var_a.reset(a_tokens[-1])

        def consume_in_b() -> None:
            b_tokens.append(var_b.set("bx"))
            var_b.reset(b_tokens[-1])

        ctx_a = Context()
        ctx_b = Context()
        ctx_a.run(consume_in_a)
        ctx_b.run(consume_in_b)

        # Act
        a_pb = ctx_a.to_protobuf()

        # Assert
        a_hex = {id_hex for id_hex in a_pb.consumed_tokens}
        assert a_tokens[0].id.hex in a_hex
        assert b_tokens[0].id.hex not in a_hex

    def test_update_with_flipped_token_ids_then_to_protobuf(
        self,
    ):
        """Test Context.update makes a merged used-token id visible in a
        subsequent Context.to_protobuf on the same Context.

        Given:
            A live Token minted under the current Context, and a
            wire protocol.Context whose ``consumed_tokens`` lists
            that Token's id (modeling a back-prop frame from a peer)
        When:
            The wire Context is merged via current_context().update
            and current_context().to_protobuf() is called
        Then:
            The resulting ``consumed_tokens`` list contains the
            merged token id — forwarding the used-state onward
            through this Context's wire emissions
        """
        # Arrange
        from wool import protocol

        var = ContextVar("c4_pin_forward", default="d")
        token = var.set("x")

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.consumed_tokens.append(token.id.hex)

        incoming = Context.from_protobuf(pb)
        current_context().update(incoming)

        # Act
        emitted = current_context().to_protobuf()

        # Assert
        assert token.id.hex in set(emitted.consumed_tokens)

    def test_to_protobuf_roundtrips_consumed_tokens(self):
        """Test Context.to_protobuf carries consumed-token state across a
        serialize/deserialize cycle.

        Given:
            A Context that has consumed a Token via ContextVar.reset
        When:
            The Context is serialized via to_protobuf and a new
            Context is reconstructed via from_protobuf
        Then:
            A pickle-alias of the consumed Token should report
            Token.used as True when inspected with the reconstructed
            Context active — the wire format preserves
            consumed-token state end-to-end
        """
        # Arrange
        var = ContextVar("proto_consumed", default="d")
        origin = Context()
        tokens: list[Token] = []

        def consume():
            t = var.set("x")
            var.reset(t)
            tokens.append(t)

        origin.run(consume)
        alias = loads(dumps(tokens[0]))

        # Act
        roundtripped = Context.from_protobuf(origin.to_protobuf())

        # Assert
        rt_sees: list[bool] = []
        roundtripped.run(lambda: rt_sees.append(alias.used))
        assert rt_sees == [True]

    def test_to_protobuf_with_custom_dumps(self):
        """Test Context.to_protobuf returns a Context with custom-serialized vars.

        Given:
            A ContextVar with a value set and a custom dumps function
        When:
            current_context().to_protobuf(serializer=custom) is called
        Then:
            It should return a protocol.Context whose vars map was
            produced by the custom serializer and whose id is a 32-char
            UUID hex string.
        """
        # Arrange
        var = ContextVar("tpb_custom_dumps", namespace="tpb")
        var.set(42)

        calls: list[object] = []

        def custom_dumps(value: object) -> bytes:
            calls.append(value)
            return b"tpb:" + str(value).encode()

        # Act
        pb = current_context().to_protobuf(
            serializer=cast(Serializer, SimpleNamespace(dumps=custom_dumps))
        )

        # Assert
        assert var.key in pb.vars
        assert pb.vars[var.key] == b"tpb:42"
        assert isinstance(pb.id, str)
        assert len(pb.id) == 32
        assert calls == [42]

    def test_from_protobuf_with_custom_loads(self):
        """Test Context.from_protobuf deserializes values via a custom loads callable.

        Given:
            A ContextVar registered in the process and a wire-form
            protocol.Context with a custom-encoded value
        When:
            Context.from_protobuf is called with a custom loads function
        Then:
            It should deserialize each value through the custom callable.
        """
        # Arrange
        from wool import protocol

        var = ContextVar("fpb_custom_loads", namespace="fpb")
        pb = protocol.Context(vars={var.key: b"custom-payload"})

        calls: list[bytes] = []

        def custom_loads(data: bytes) -> object:
            calls.append(data)
            return "decoded-" + data.decode()

        # Act
        reconstructed = Context.from_protobuf(
            pb, serializer=cast(Serializer, SimpleNamespace(loads=custom_loads))
        )

        # Assert
        assert reconstructed[var] == "decoded-custom-payload"
        assert calls == [b"custom-payload"]


def test_current_context_with_set_vars():
    """Test current_context() returns the live Context for the scope.

    Given:
        A ContextVar with an explicit value set
    When:
        current_context() is called twice from the same scope
    Then:
        The returned Context contains the var with its value and
        both calls yield the same Context instance (idempotent
        within a scope)
    """
    # Arrange
    var = ContextVar("cur_ctx", default=0)
    var.set(1)

    # Act
    ctx = current_context()

    # Assert
    assert ctx[var] == 1
    assert current_context() is ctx


def test_copy_context_with_set_vars():
    """Test copy_context() snapshots vars and assigns a fresh chain id.

    Given:
        A ContextVar with an explicit value set in the live Context
    When:
        copy_context() is called
    Then:
        The snapshot contains the var's value but its id differs
        from the live Context's id — the copy is an independent
        logical chain
    """
    # Arrange
    var = ContextVar("copy_ctx", default=0)
    var.set(1)
    live_id = current_context().id

    # Act
    snapshot = copy_context()

    # Assert
    assert snapshot[var] == 1
    assert snapshot.id != live_id


def test_copy_context_chain_id_uniqueness():
    """Test successive copy_context() calls each get a distinct id.

    Given:
        No mutation to the live Context between calls
    When:
        copy_context() is called twice
    Then:
        The two snapshots have distinct ids
    """
    # Act
    a = copy_context()
    b = copy_context()

    # Assert
    assert a.id != b.id


def test_carries_state_with_empty_protocol_context():
    """Test carries_state returns False for an empty wire context.

    Given:
        A protocol.Context with no vars and no consumed_tokens
    When:
        carries_state is invoked on it
    Then:
        It returns False — the receiver can skip the
        Context.from_protobuf decode for empty wire frames
    """
    # Arrange
    from wool import protocol
    from wool.runtime.context import carries_state

    pb = protocol.Context(id=uuid.uuid4().hex)

    # Act
    result = carries_state(pb)

    # Assert
    assert result is False


def test_carries_state_with_var_bindings():
    """Test carries_state returns True when the wire context has at
    least one var binding.

    Given:
        A protocol.Context with one entry in ``vars`` and an empty
        consumed_tokens list
    When:
        carries_state is invoked on it
    Then:
        It returns True — the var bindings need to be merged on
        receive via Context.update
    """
    # Arrange
    from wool import protocol
    from wool.runtime.context import carries_state

    pb = protocol.Context(id=uuid.uuid4().hex)
    pb.vars["some_namespace:some_name"] = b"opaque-bytes"

    # Act
    result = carries_state(pb)

    # Assert
    assert result is True


def test_carries_state_with_consumed_tokens_only():
    """Test carries_state returns True when only consumed_tokens is
    populated and var bindings are empty.

    Given:
        A protocol.Context with no var bindings but a non-empty
        consumed_tokens list (modeling a back-prop response that
        carries only used-token state)
    When:
        carries_state is invoked on it
    Then:
        It returns True — the consumed token ids must be applied
        on merge to flip the corresponding live tokens' used flags
    """
    # Arrange
    from wool import protocol
    from wool.runtime.context import carries_state

    pb = protocol.Context(id=uuid.uuid4().hex)
    pb.consumed_tokens.append(uuid.uuid4().hex)

    # Act
    result = carries_state(pb)

    # Assert
    assert result is True


def test_attach_with_fresh_context():
    """Test attach installs the given Context as the current scope's
    binding and returns a token usable with detach.

    Given:
        A fresh wool.Context distinct from whatever the surrounding
        scope already has bound (the autouse fixture installs an
        outer Context).
    When:
        attach is called with the new Context.
    Then:
        peek() observes the new Context, current_context() agrees,
        and the returned token is non-None so callers can later
        detach to restore the prior binding.
    """
    # Arrange
    from wool.runtime.context import attach
    from wool.runtime.context import detach
    from wool.runtime.context import peek

    new_ctx = Context()

    # Act
    token = attach(new_ctx)
    try:
        observed_peek = peek()
        observed_current = current_context()
    finally:
        detach(token)

    # Assert
    assert observed_peek is new_ctx
    assert observed_current is new_ctx
    assert token is not None


def test_detach_restores_previous_ctx_when_slot_was_bound():
    """Test detach reinstalls the prior Context after an attach over an
    already-bound scope.

    Given:
        An outer wool.Context bound on the current scope (provided by
        the autouse fixture) and a second Context that gets attached
        on top.
    When:
        detach is called with the token returned by the inner attach.
    Then:
        peek() observes the outer Context again — the inner attach
        is undone and the prior binding is restored, not popped.
    """
    # Arrange
    from wool.runtime.context import attach
    from wool.runtime.context import detach
    from wool.runtime.context import peek

    outer = peek()
    assert outer is not None
    inner = Context()
    token = attach(inner)

    # Act
    detach(token)

    # Assert
    assert peek() is outer


@pytest.mark.asyncio
async def test_detach_pops_binding_when_slot_was_unset():
    """Test detach removes the binding entirely when attach installed
    over an unset scope slot.

    Given:
        An asyncio task whose scope has no Context registered (a
        fresh asyncio.create_task child built without the wool task
        factory installed — see test_no_side_effect_on_lazy_register
        for the same pattern).
    When:
        attach installs a Context, then detach is called with the
        returned token.
    Then:
        peek() observes None inside the task afterward — detach
        popped the slot rather than rewriting it to an unrelated
        Context.
    """
    # Arrange
    from wool.runtime.context import attach
    from wool.runtime.context import detach
    from wool.runtime.context import peek

    observed: list[Context | None] = []

    async def body():
        ctx = Context()
        token = attach(ctx)
        detach(token)
        observed.append(peek())

    # Act
    await asyncio.create_task(body())

    # Assert
    assert observed == [None]


def test_detach_with_consumed_token():
    """Test detach raises RuntimeError when called a second time with
    a token that has already been consumed.

    Given:
        A token returned by attach(ctx) which has already been passed
        once to detach (the install/restore cycle is complete).
    When:
        detach is called a second time with the same token.
    Then:
        RuntimeError is raised — the guard prevents a stale token
        from re-running the restore step, which would otherwise
        corrupt the registry by reinstalling the prior Context over
        whatever has since been bound to the scope.
    """
    # Arrange
    from wool.runtime.context import attach
    from wool.runtime.context import detach

    token = attach(Context())
    detach(token)

    # Act & assert
    with pytest.raises(RuntimeError, match="already consumed"):
        detach(token)


@pytest.mark.asyncio
async def test_peek_when_scope_has_no_bound_ctx():
    """Test peek returns None on a scope without a registered Context
    and does not lazy-register one as a side effect.

    Given:
        An asyncio task whose scope has no Context registered (a
        fresh asyncio.create_task child built without wool's task
        factory installed).
    When:
        peek() is called from inside that task, then a second time.
    Then:
        Both calls return None — peek is non-registering, in
        contrast to current_context() which materializes a Context
        on first access.
    """
    # Arrange
    from wool.runtime.context import peek

    observed: list[Context | None] = []

    async def body():
        observed.append(peek())
        observed.append(peek())

    # Act
    await asyncio.create_task(body())

    # Assert
    assert observed == [None, None]


@pytest.mark.asyncio
async def test_register_is_visible_to_current_context():
    """Test current_context sees a task pre-registered out-of-band.

    Given:
        An asyncio.Task with a wool.Context pre-registered via
        wool.runtime.context.register (the surface worker/service
        uses to bind a worker-loop task's Context).
    When:
        The task's coroutine calls current_context().
    Then:
        It receives the pre-registered Context.
    """
    from wool.runtime.context import register

    # Arrange
    seeded = Context()
    captured: list[Context] = []

    async def body():
        captured.append(current_context())

    loop = asyncio.get_running_loop()
    task = asyncio.Task(body(), loop=loop)
    register(task, seeded)

    # Act
    await task

    # Assert
    assert captured == [seeded]


@pytest.mark.asyncio
async def test_register_when_task_already_bound():
    """Test double register raises ContextAlreadyBound.

    Given:
        An asyncio.Task already bound to a Context via register.
    When:
        register is called a second time for the same task.
    Then:
        ContextAlreadyBound is raised and the original binding
        is preserved.
    """
    from wool.runtime.context import ContextAlreadyBound
    from wool.runtime.context import register

    # Arrange
    first = Context()
    second = Context()

    async def body():
        pass

    loop = asyncio.get_running_loop()
    task = asyncio.Task(body(), loop=loop)
    register(task, first)

    # Act & assert
    try:
        with pytest.raises(ContextAlreadyBound):
            register(task, second)
    finally:
        await task


@pytest.mark.asyncio
async def test_create_bound_task_with_explicit_ctx():
    """Test create_bound_task pre-binds the given Context and bypasses
    the copy-on-fork path the normal task factory would take.

    Given:
        An event loop with wool's task factory installed and a
        parent scope holding a ContextVar binding that the factory's
        fork path would normally propagate to child tasks.
    When:
        create_bound_task is called with a fresh (empty) target
        Context distinct from the parent's.
    Then:
        The child sees the target Context directly — same identity,
        and crucially without the parent's var binding — proving
        the fork path was skipped rather than taken.
    """
    from wool.runtime.context import create_bound_task
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    install_task_factory(loop)

    sentinel_var = ContextVar("bound_task_fork_sentinel", default="default")
    sentinel_var.set("parent-value")

    target = Context()
    observed_ctx: list[Context] = []
    observed_value: list[str] = []

    async def body():
        observed_ctx.append(current_context())
        observed_value.append(sentinel_var.get())

    # Act
    task = create_bound_task(loop, body(), target)
    await task

    # Assert
    assert observed_ctx == [target]
    # A forked child would have inherited "parent-value" via
    # parent.copy(); the bound child sees the fresh target's empty
    # state and falls through to the var's default.
    assert observed_value == ["default"]


@pytest.mark.asyncio
async def test_create_task_inside_parent_context_scope():
    """Test the child task spawned via asyncio.create_task inherits a fork
    of the parent's current_context (not an empty Context).

    Given:
        An event loop with wool's task factory installed, a parent
        task that holds a non-empty Context carrying a ContextVar
        binding
    When:
        The parent calls asyncio.create_task to spawn a child
    Then:
        The child's current_context should be a distinct Context
        (fresh chain id) but carrying the same var binding the
        parent set — locking in the fork-on-spawn contract that
        depends on ``asyncio.current_task()`` resolving to the
        parent inside the task-factory callback
    """
    from wool.runtime.context import install_task_factory

    loop = asyncio.get_running_loop()
    install_task_factory(loop)

    var = ContextVar("fork_invariant_probe", default="d")
    var.set("parent-value")
    parent_ctx = current_context()

    captured: list[tuple[uuid.UUID, str]] = []

    async def child() -> None:
        child_ctx = current_context()
        captured.append((child_ctx.id, var.get()))

    await asyncio.create_task(child())

    assert len(captured) == 1
    child_id, child_value = captured[0]
    assert child_id != parent_ctx.id  # fork mints a fresh chain id
    assert child_value == "parent-value"  # but carries parent's var state


@pytest.mark.asyncio
async def test_create_bound_task_falls_back_when_no_wool_factory_installed():
    """Test create_bound_task binds ctx via the fallback factory path.

    Given:
        An event loop whose task factory has NOT been wrapped by
        install_task_factory (loop.get_task_factory() returns None,
        so the wool-wrapped fast path is skipped).
    When:
        create_bound_task(loop, coro, target_ctx) is invoked and the
        resulting task is awaited.
    Then:
        The coroutine runs under target_ctx — the fallback factory
        path still binds the supplied Context via register(task, ctx)
        after the default factory creates the task.
    """
    from wool.runtime.context import create_bound_task

    # Arrange
    loop = asyncio.get_running_loop()
    # Ensure no wool factory is wrapped around the loop; this pins
    # create_bound_task onto the _default_task_factory branch.
    loop.set_task_factory(None)
    assert loop.get_task_factory() is None

    target = Context()
    captured: list[Context] = []

    async def body():
        captured.append(current_context())

    # Act
    task = create_bound_task(loop, body(), target)
    await task

    # Assert
    assert captured == [target]


@pytest.mark.asyncio
async def test_create_bound_task_invokes_non_wool_user_factory():
    """Test create_bound_task routes through a user-installed factory.

    Given:
        An event loop whose task factory has been set to a non-wool
        user factory (observable via a call counter on the factory).
    When:
        create_bound_task(loop, coro, target_ctx) is invoked and the
        resulting task is awaited.
    Then:
        The user factory is called exactly once, the coroutine runs
        under target_ctx, and the default asyncio.Task factory is not
        invoked as a silent bypass — user factories (task-name
        propagation, observability hooks, etc.) are preserved.
    """
    from wool.runtime.context import create_bound_task

    # Arrange
    loop = asyncio.get_running_loop()
    user_factory_calls: list[Coroutine] = []

    def user_factory(
        loop: asyncio.AbstractEventLoop,
        coro: Coroutine,
        **kwargs,
    ) -> asyncio.Task:
        user_factory_calls.append(coro)
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(user_factory)  # type: ignore[arg-type]
    assert loop.get_task_factory() is user_factory

    target = Context()
    captured: list[Context] = []

    async def body():
        captured.append(current_context())

    # Act
    task = create_bound_task(loop, body(), target)
    await task

    # Assert
    assert len(user_factory_calls) == 1
    assert captured == [target]


@pytest.mark.asyncio
async def test_install_task_factory_idempotent():
    """Test install_task_factory is a no-op when already installed.

    Given:
        install_task_factory has been called on the running loop
    When:
        install_task_factory is called again
    Then:
        It should return without error (idempotent)
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    install_task_factory()

    # Act & assert — no error
    install_task_factory()


@pytest.mark.asyncio
async def test_install_task_factory_with_existing_factory():
    """Test install_task_factory wraps an existing factory.

    Given:
        A custom task factory already set on the loop
    When:
        install_task_factory is called
    Then:
        It should wrap the existing factory, creating tasks via the
        original while also seeding wool Context on the child
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    calls = []

    def custom_factory(loop, coro, **kwargs):
        calls.append("custom")
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)

    # Act
    install_task_factory()

    var = ContextVar("compose_test", namespace="test_compose")
    var.set("parent_value")

    async def child():
        return var.get("missing")

    result = await asyncio.create_task(child())

    # Assert
    assert len(calls) > 0  # custom factory was called
    assert result == "parent_value"  # wool context inherited

    # Cleanup
    loop.set_task_factory(None)


@pytest.mark.asyncio
async def test_install_task_factory_idempotent_over_composed():
    """Test install_task_factory is a no-op when a wool-composed factory is installed.

    Given:
        A user factory was set on the loop and install_task_factory
        composed around it
    When:
        install_task_factory is called again
    Then:
        The second call recognizes the _wool_wrapped marker and
        returns without replacing the composed factory
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()

    def custom_factory(loop, coro, **kwargs):
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)
    install_task_factory()
    composed = loop.get_task_factory()

    # Act
    install_task_factory()

    # Assert
    assert loop.get_task_factory() is composed

    # Cleanup
    loop.set_task_factory(None)


@pytest.mark.asyncio
async def test_install_task_factory_on_fresh_loop(caplog):
    """Test install_task_factory logs a fresh-install message on an empty loop.

    Given:
        A running event loop with no task factory set
    When:
        install_task_factory runs once
    Then:
        A debug record naming the "installed" path is emitted
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)

    # Act
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()

    # Assert
    assert any("wool task factory installed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_install_task_factory_when_recalled(caplog):
    """Test a second install on the same loop logs the already-installed path.

    Given:
        A running event loop with wool's factory already installed
    When:
        install_task_factory runs a second time
    Then:
        A debug record naming the "already installed" path is emitted
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)
    install_task_factory()

    # Act
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()

    # Assert
    assert any("already installed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_install_task_factory_with_user_factory_present(caplog):
    """Test install over a non-wool user factory logs the compose path.

    Given:
        A running event loop with a non-wool user task factory in place
    When:
        install_task_factory runs
    Then:
        A debug record naming the "composed with existing factory"
        path is emitted
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)

    def custom_factory(loop, coro, **kwargs):
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)

    # Act
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()

    # Assert
    assert any("composed with existing factory" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_install_task_factory_when_recalled_over_composed(
    caplog,
):
    """Test a second install over a composed factory logs the already-composed path.

    Given:
        A running event loop with wool's factory already composed
        over a user factory
    When:
        install_task_factory runs a second time
    Then:
        A debug record naming the "composed task factory already
        installed" path is emitted
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)

    def custom_factory(loop, coro, **kwargs):
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)
    install_task_factory()

    # Act
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()

    # Assert
    assert any(
        "composed task factory already installed" in r.message for r in caplog.records
    )
