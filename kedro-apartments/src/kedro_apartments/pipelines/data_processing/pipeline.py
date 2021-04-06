from kedro.pipeline import Pipeline, node
from .nodes import *


def create_apartment_enrichment_pipeline():
    return Pipeline(
        [
            node(
                func=concat_partitions,
                inputs=[
                    "apartments",
                    "params:from_date",
                    "params:to_date",
                ],
                outputs="concatenated_result",
                name="concat_partitions_node",
            ),
            node(
                func=merge_insert_update_time,
                inputs="concatenated_result",
                outputs="merge_insert_update_time",
                name="merge_insert_update_time_node",
            ),
            node(
                func=drop_duplicates,
                inputs="merge_insert_update_time",
                outputs="drop_duplicates",
                name="drop_duplicates_node",
            ),
            node(
                func=floor_to_sqm,
                inputs="drop_duplicates",
                outputs="floor_to_sqm",
                name="floor_to_sqm_node",
            ),
            node(
                func=floor_to_built,
                inputs="floor_to_sqm",
                outputs="floor_to_built",
                name="floor_to_built_node",
            ),
            node(
                func=swap_space_sqm,
                inputs="floor_to_built",
                outputs="swap_space_sqm",
                name="swap_space_sqm_node",
            ),
            node(
                func=extract_floor,
                inputs="swap_space_sqm",
                outputs="extract_floor",
                name="extract_floor_node",
            ),
            node(
                func=extract_area,
                inputs="extract_floor",
                outputs="extract_area",
                name="extract_area_node",
            ),
            node(
                func=extract_price,
                inputs="extract_area",
                outputs="extract_price",
                name="extract_price_node",
            ),
        ]
    )
