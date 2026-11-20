"""Small scientific definitions with ordinary fixed artifact inputs."""

from __future__ import annotations

from typing import ClassVar

from artisan.composites import CompositeContext
from artisan.schemas import InputSpec
from fixtures.input_contract_operations import NestedSharedOffset, SharedOffset, metric


class DefaultOffset(SharedOffset):
    name = "fixed_default_offset"
    inputs: ClassVar = {
        **SharedOffset.inputs,
        "offset": InputSpec(
            artifact_type="metric", materialize=False, default=(metric(10),)
        ),
    }


class OtherDefaultOffset(DefaultOffset):
    name = "fixed_other_default_offset"
    inputs: ClassVar = {
        **DefaultOffset.inputs,
        "offset": InputSpec(
            artifact_type="metric", materialize=False, default=(metric(20),)
        ),
    }


class DefaultPair(NestedSharedOffset):
    name = "fixed_default_pair"

    def compose(self, ctx: CompositeContext) -> None:
        first = ctx.call(
            DefaultOffset, name="first", inputs={"source": ctx.input("source")}
        )
        second = ctx.call(
            OtherDefaultOffset, name="second", inputs={"source": ctx.input("source")}
        )
        ctx.output("metrics", [first.output("metrics"), second.output("metrics")])


class NestedDefaultPair(NestedSharedOffset):
    name = "fixed_nested_default_pair"

    def compose(self, ctx: CompositeContext) -> None:
        nested = ctx.call(
            DefaultPair, name="nested", inputs={"source": ctx.input("source")}
        )
        last = ctx.call(
            DefaultOffset, name="last", inputs={"source": ctx.input("source")}
        )
        ctx.output("metrics", [nested.output("metrics"), last.output("metrics")])
