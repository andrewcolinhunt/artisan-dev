"""Tests for compute_chain_spec_id."""

from __future__ import annotations

from artisan.utils.hashing import compute_chain_spec_id


class TestComputeChainSpecId:
    def test_deterministic(self) -> None:
        ops = [("gen", {"n": 10}), ("transform", {"mode": "a"})]
        inputs = {"data": ["a" * 32]}
        id1 = compute_chain_spec_id(ops, inputs)
        id2 = compute_chain_spec_id(ops, inputs)
        assert id1 == id2
        assert len(id1) == 32

    def test_param_sensitivity(self) -> None:
        ops1 = [("gen", {"n": 10})]
        ops2 = [("gen", {"n": 20})]
        inputs = {"data": ["a" * 32]}
        assert compute_chain_spec_id(ops1, inputs) != compute_chain_spec_id(
            ops2, inputs
        )

    def test_order_sensitivity(self) -> None:
        ops1 = [("gen", None), ("transform", None)]
        ops2 = [("transform", None), ("gen", None)]
        inputs = {"data": ["a" * 32]}
        assert compute_chain_spec_id(ops1, inputs) != compute_chain_spec_id(
            ops2, inputs
        )

    def test_input_sensitivity(self) -> None:
        ops = [("gen", None)]
        inputs1 = {"data": ["a" * 32]}
        inputs2 = {"data": ["b" * 32]}
        assert compute_chain_spec_id(ops, inputs1) != compute_chain_spec_id(
            ops, inputs2
        )

    def test_empty_inputs(self) -> None:
        ops = [("gen", {"n": 5})]
        result = compute_chain_spec_id(ops, {})
        assert len(result) == 32

    def test_none_params(self) -> None:
        ops = [("gen", None)]
        result = compute_chain_spec_id(ops, {})
        assert len(result) == 32
