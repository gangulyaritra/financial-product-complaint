from typing import List
from pyspark import keyword_only
from pyspark.ml import Estimator
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    Param,
    Params,
    TypeConverters,
    HasOutputCols,
    HasInputCols,
)
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
from pyspark.sql.functions import desc, col, abs
from pyspark.sql.types import TimestampType, LongType
from src.config.spark_manager import spark_session


class FrequencyEncoder(
    Estimator, HasInputCols, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable
):
    frequencyInfo = Param(
        Params._dummy(),
        "getfrequencyInfo",
        "getfrequencyInfo",
        typeConverter=TypeConverters.toList,
    )

    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None):
        super(FrequencyEncoder, self).__init__()
        kwargs = self._input_kwargs

        self.frequencyInfo = Param(self, "frequencyInfo", "")
        self._setDefault(frequencyInfo="")
        self.setParams(**kwargs)

    def setfrequencyInfo(self, frequencyInfo: list):
        return self._set(frequencyInfo=frequencyInfo)

    def getfrequencyInfo(self):
        return self.getOrDefault(self.frequencyInfo)

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCols(self, value: List[str]):
        return self._set(inputCol=value)

    def setOutputCols(self, value: List[str]):
        return self._set(outputCols=value)

    def _fit(self, dataframe: DataFrame):
        input_columns = self.getInputCols()
        output_columns = self.getOutputCols()
        replace_info = []

        for column, new_column in zip(input_columns, output_columns):
            freq = (
                dataframe.select(col(column).alias(f"g_{column}"))
                .groupby(f"g_{column}")
                .count()
                .withColumn(new_column, col("count"))
            )
            freq = freq.drop("count")
            replace_info.append(freq.collect())

        self.setfrequencyInfo(frequencyInfo=replace_info)
        estimator = FrequencyEncoderModel(
            inputCols=input_columns, outputCols=output_columns
        )
        estimator.setfrequencyInfo(frequencyInfo=replace_info)
        return estimator


class FrequencyEncoderModel(FrequencyEncoder, Transformer):
    def __init__(
        self,
        inputCols: List[str] = None,
        outputCols: List[str] = None,
    ):
        super(FrequencyEncoderModel, self).__init__(
            inputCols=inputCols, outputCols=outputCols
        )

    def _transform(self, dataframe: DataFrame):
        inputCols = self.getInputCols()
        outputCols = self.getOutputCols()
        freqInfo = self.getfrequencyInfo()

        for in_col, out_col, freq_info in zip(inputCols, outputCols, freqInfo):
            frequency_dataframe: DataFrame = spark_session.createDataFrame(freq_info)

            columns = frequency_dataframe.columns
            dataframe = dataframe.join(
                frequency_dataframe,
                on=dataframe[in_col] == frequency_dataframe[columns[0]],
            )
            dataframe = dataframe.drop(columns[0])

            if out_col not in dataframe.columns:
                dataframe = dataframe.withColumn(out_col, col(columns[1]))
                dataframe = dataframe.drop(columns[1])
        return dataframe


class DerivedFeatureGenerator(
    Transformer,
    HasInputCols,
    HasOutputCols,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None):
        super(DerivedFeatureGenerator, self).__init__()
        kwargs = self._input_kwargs

        self.second_within_day = 60 * 60 * 24
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setInputCols(self, value: List[str]):
        return self._set(inputCol=value)

    def setOutputCols(self, value: List[str]):
        return self._set(outputCols=value)

    def _fit(self):
        return self

    def _transform(self, dataframe: DataFrame):
        inputCols = self.getInputCols()

        for column in inputCols:
            dataframe = dataframe.withColumn(column, col(column).cast(TimestampType()))

        dataframe = dataframe.withColumn(
            self.getOutputCols()[0],
            abs(col(inputCols[1]).cast(LongType()) - col(inputCols[0]).cast(LongType()))
            / (self.second_within_day),
        )
        return dataframe


class FrequencyImputer(
    Estimator, HasInputCols, HasOutputCols, DefaultParamsReadable, DefaultParamsWritable
):
    topCategorys = Param(
        Params._dummy(),
        "getTopCategorys",
        "getTopCategorys",
        typeConverter=TypeConverters.toListString,
    )

    @keyword_only
    def __init__(self, inputCols: List[str] = None, outputCols: List[str] = None):
        super(FrequencyImputer, self).__init__()
        self.topCategorys = Param(self, "topCategorys", "")
        self._setDefault(topCategorys="")

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols: List[str] = None, outputCols: List[str] = None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setTopCategorys(self, value: List[str]):
        return self._set(topCategorys=value)

    def getTopCategorys(self):
        return self.getOrDefault(self.topCategorys)

    def setInputCols(self, value: List[str]):
        return self._set(inputCols=value)

    def setOutputCols(self, value: List[str]):
        return self._set(outputCols=value)

    def _fit(self, dataset: DataFrame):
        inputCols = self.getInputCols()
        topCategorys = []

        for column in inputCols:
            categoryCountByDesc = (
                dataset.groupBy(column)
                .count()
                .filter(f"{column} is not null")
                .sort(desc("count"))
            )
            topCat = categoryCountByDesc.take(1)[0][column]
            topCategorys.append(topCat)

        self.setTopCategorys(value=topCategorys)

        estimator = FrequencyImputerModel(
            inputCols=self.getInputCols(), outputCols=self.getOutputCols()
        )
        estimator.setTopCategorys(value=topCategorys)
        return estimator


class FrequencyImputerModel(FrequencyImputer, Transformer):
    def __init__(
        self,
        inputCols: List[str] = None,
        outputCols: List[str] = None,
    ):
        super(FrequencyImputerModel, self).__init__(
            inputCols=inputCols, outputCols=outputCols
        )

    def _transform(self, dataset: DataFrame):
        topCategorys = self.getTopCategorys()
        outputCols = self.getOutputCols()

        updateMissingValue = dict(zip(outputCols, topCategorys))
        inputCols = self.getInputCols()

        for outputColumn, inputColumn in zip(outputCols, inputCols):
            dataset = dataset.withColumn(outputColumn, col(inputColumn))

        dataset = dataset.na.fill(updateMissingValue)
        return dataset
