from dataplatform.setting.spark_session import get_spark
from dataplatform.etl_tools import Slicer, SliceRegistry
from dataplatform.etl_tools import SCD2Loader


def run() -> None:
    spark = get_spark()
    registry = SliceRegistry('load_gold_dim_video')
    slicer = Slicer(
        session=spark,
        registry=registry,
        table='local.silver.s_video',
        slice_column='processed_dttm'
    )

    spark.sql('''
        create table if not exists local.gold.dim_video (
            video_id string,
            created_at timestamp,
            title string,
            description string,
            category_id string,
            live_broadcast_content string,
            default_language string,
            duration_sec int,
            dimension string,
            definition string,
            caption boolean,
            license boolean,
            content_rating string,
            projection string,
            privacy_status string,
            upload_status string,
            is_available boolean,
            valid_from_dttm timestamp,
            valid_to_dttm timestamp,
            processed_dttm timestamp
        ) using iceberg
        partitioned by (days(valid_from_dttm))
    ''')

    local_silver_h_video = spark.table('local.silver.h_video')
    local_silver_s_video = slicer.run()

    to_load = local_silver_h_video.join(
        other=local_silver_s_video,
        on='video_id',
        how='inner'
    ).select(
        'video_id',
        'created_at',
        'title',
        'description',
        'category_id',
        'live_broadcast_content',
        'default_language',
        'duration_sec',
        'dimension',
        'definition',
        'caption',
        'license',
        'content_rating',
        'projection',
        'privacy_status',
        'upload_status',
        'is_available',
        'valid_from_dttm'
    )

    if not to_load.isEmpty():
        loader = SCD2Loader(
            input=to_load,
            output='local.gold.dim_video',
            business_key='video_id',
            key_columns=['video_id', 'valid_from_dttm'],
            mode='upsert',
            rebuild_history_mode='from_dt'
        )
        loader.run()
    slicer.commit()
