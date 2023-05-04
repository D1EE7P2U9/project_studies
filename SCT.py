import boto3
import botocore.exceptions

from .base_step import Step
from .observability import StepMetric
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.dataframe import *
from pyspark.sql import Window
from pyspark.sql import functions as F


@Step(
    type="scd2-incremental-historization",
    props_schema={
        "type": "object",
        "required": [
            "target", "primaryKey", "recordKey"
        ],
        "properties": {
            "target": {"type": "string"},
            "path": {"type": "string"},
            "bucket": {"type": "string"},
            "primaryKey": {"type": "string"},
            "prefix": {"type": "string"},
            "emitmetric": {"type": "boolean"},
            "recordKey": {"type": "string"},
            "isHudiDataset": {"type": "boolean"},
            "actionFlagColumn": {"type": "string"},
            "actionDateColumn": {"type": "string"},
            "extraOrderingColumns": {"type": "object"}
        },
        "oneOf": [
            {"required": ["path"]},
            {"required": ["bucket", "prefix"]}
        ]
    }
)
class SCD2IncrementalHistorization:
    """
        Handler for historization of SCD2 of incremental data.
        technical fields created: int_tec_from_dt, int_tec_to_dt, curr_flg, del_flg
        For usage information check: https://confluence.biscrum.com/display/DMSDATAENG/SCD2+Incremental+Historization
    """

    def run_step(self, spark, config, context, glueContext=None):
        self.props = self.param_parser.parse_parameters(self.props)
        spark.conf.set('spark.sql.legacy.timeParserPolicy','LEGACY')
        prefix = f"{self.name} [{self.type}]"
        job_name = config.args.get("JOB_NAME")

        self.logger.info(f"{prefix} SCD2 HISTORIZATION OF INCREMENTAL {self.props.get('target')}")

        # read the previous step data to historize it
        df = context.df(self.props.get("target"))

        path = ""
        if self.props.get("bucket"):
            path = f"s3://{self.props.get('bucket')}/{self.props.get('prefix')}/"
        elif self.props.get("path"):
            path = self.props.get("path")

        self.logger.info(f"{prefix} Path: {path}")

        # get the initial partition keys by the user
        pr_tab_hkey = self.props.get("primaryKey")
        pr_tab_hist_hkey = self.props.get("recordKey")

       # Get the actionFlag, actionDate column names.
        action_flag = self.props.get("actionFlagColumn")
        action_date = self.props.get("actionDateColumn")

        # Special case with Update counter.
        extra_ordering_columns = self.props.get("extraOrderingColumns", None)

        df = df.withColumn("CreatedDate",F.when(F.col("CreatedDate")\
                .rlike('\d{1,2}\/\d{1,2}\/\d{4}/\d{2}\/\d{2}\/\d{2}/\d{6}')\
                ,F.to_timestamp(df["CreatedDate"],'MM-dd-yyyy HH:mm:ss.SSSSSS'))\
                .otherwise(F.to_timestamp(df["CreatedDate"],'MM/dd/yyyy HH:mm:ss.SSSSSS')))
        
        # Historize the incremental data as left side table
        window_business_key = Window.partitionBy(pr_tab_hkey).orderBy(df[action_date].asc())

        if self.props.get("isHudiDataset"):
            df_1 = df.select([df[cols] for cols in df.columns if
                              cols not in {"_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
                                           "_hoodie_partition_path", "_hoodie_file_name", "moduleKey"}])
        else:

            # Convert timestamp to date for populating begining of validity interval
            df = df.withColumn("int_tec_from_dt", to_date(df[action_date], 'yyyy-MM-dd'))
            
            # df = df.withColumn("int_tec_from_dt",to_date(df[action_date],'yyyy-MM-dd'))
            # Creating a new timestamp column
            df = df.withColumn("CreatedDate_Timestamp",F.when(F.col(action_date)\
                    .rlike('\d{1,2}\/\d{1,2}\/\d{4}/\d{2}\/\d{2}\/\d{2}/\d{6}'),\
                    F.unix_timestamp(df[action_date],'MM-dd-yyyy HH:mm:ss.SSSSSS'))\
                    .otherwise(F.unix_timestamp(df[action_date],'MM/dd/yyyy HH:mm:ss.SSSSSS')))

            # Removing multiple actions happening in a day and taking only last one of same business key
            window_hist_key = Window.partitionBy(pr_tab_hist_hkey) \
                .orderBy(df[action_date].desc(),
                         *build_extra_ordering_columns(df, extra_ordering_columns))

            df = df.withColumn("row_number", row_number().over(window_hist_key)).filter(col("row_number") == 1) \
                .drop("row_number")

            df_1 = df.withColumn("int_tec_to_dt", lead(df["int_tec_from_dt"]).over(window_business_key)) \
                .withColumn("int_tec_to_dt", when(col('int_tec_to_dt').isNull(),
                                                  to_date(lit('9999-12-31'), 'yyyy-MM-dd')).otherwise(
                col('int_tec_to_dt'))) \
                .withColumn("curr_flg", when(col("int_tec_to_dt") == '9999-12-31', 1).otherwise(0)) \
                .withColumn("int_tec_to_dt", when(col("int_tec_to_dt") != '9999-12-31',
                                                  date_sub(col("int_tec_to_dt"), 1)).otherwise(
                col("int_tec_to_dt"))) #\
                #.withColumn("del_flg", when(col(action_flag) == 'D', 1).otherwise(0))

        # logic to check whether there (right side) is already a historized table or not
        client = boto3.client('s3')

        result = client.list_objects(Bucket=self.props.get('bucket'), Prefix=self.props.get('prefix'))

        if 'Contents' not in result:
            self.logger.info(f"{prefix} First time historization should be done")

            df_write = df_1

            mode = 'overwrite'
        else:
            self.logger.info(f"{prefix} There is already right hand historized")

            # read the already historized data
            table_hist = spark \
                .read \
                .format('org.apache.hudi') \
                .load(path + '/*')

            # Discard Join Condition
            discard_join_cond11 = [table_hist[pr_tab_hkey] == df_1[pr_tab_hkey],table_hist[pr_tab_hist_hkey] == df_1[pr_tab_hist_hkey], table_hist.curr_flg == 1]
            discard_df = (table_hist.join(df_1, discard_join_cond11))
            df_1 = df_1.join(discard_df,on=pr_tab_hkey,how='leftanti')

            df_2 = df_1.withColumn("row_number", row_number().over(window_business_key)) \
                .filter(col("row_number") == 1) \
                .drop(col("row_number"))

            # join condition
            join_cond = [table_hist[pr_tab_hkey] == df_2[pr_tab_hkey], table_hist.curr_flg == 1]

            # join historized table with new changes and making the current flag to zero for those that new changes are comming
            table_to_update_df = (table_hist \
                                  .join(df_2, join_cond) \
                                  .select(
                [table_hist[cols] for cols in table_hist.columns if cols not in {"int_tec_to_dt",
                                                                                 "curr_flg", "_hoodie_commit_time",
                                                                                 "_hoodie_commit_seqno",
                                                                                 "_hoodie_record_key",
                                                                                 "_hoodie_partition_path",
                                                                                 "_hoodie_file_name", "moduleKey"}] + [
                    date_sub(df_2.int_tec_from_dt, 1).alias('int_tec_to_dt')])
                                  .withColumn('curr_flg', lit(0))
                                  )

            # Projection of incremental should be the same as existing table in hist
            df_1 = df_1.select(
                [df_1[cols] for cols in table_hist.columns if cols not in {"_hoodie_commit_time",
                                                                                 "_hoodie_commit_seqno",
                                                                                 "_hoodie_record_key",
                                                                                 "_hoodie_partition_path",
                                                                                 "_hoodie_file_name", "moduleKey"}]
            )

            # union with the other pr_tab_hkey that are not already historized
            merged_df = df_1.unionByName(table_to_update_df)

            # remove incorrect validity interval happens when data corresponding to one day is ingested in two different days
            df_write = merged_df.filter(merged_df.int_tec_from_dt <= merged_df.int_tec_to_dt)

            mode = 'append'

        context.register_df(self.name, df_write)
        context.register_pipeline_var(self.name + '.mode', mode)

        if self.props.get("emitmetric") == True:
            self.emit_metric(StepMetric(name=f"{job_name}/{self.name}/count", unit="NbRecord",
                                    value=df_write.rdd.countApprox(timeout=800, confidence=0.5)))


def build_extra_ordering_columns(df, extra_columns):
    extra_order_by = []

    if not extra_columns:
        return extra_order_by

    for column, ordering in extra_columns.items():

        if ordering.lower().startswith("asc"):

            if "nulls last" in ordering.lower():
                extra_order_by.append(df[column].asc_nulls_last())
            elif "nulls first" in ordering.lower():
                extra_order_by.append(df[column].asc_nulls_first())
            else:
                extra_order_by.append(df[column].asc())

        elif ordering.lower().startswith("desc"):

            if "nulls last" in ordering.lower():
                extra_order_by.append(df[column].desc_nulls_last())
            elif "nulls first" in ordering.lower():
                extra_order_by.append(df[column].desc_nulls_first())
            else:
                extra_order_by.append(df[column].desc())
    return extra_order_by
