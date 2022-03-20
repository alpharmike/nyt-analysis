from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover
from pyspark.sql.dataframe import DataFrame


def tokenize_dataframe(dataframe, input_col, output_col, regex=False, pattern=None) -> DataFrame:
    if regex:
        tokenizer = RegexTokenizer(inputCol=input_col, outputCol=output_col, pattern=pattern)
    else:
        tokenizer = Tokenizer(inputCol=input_col, outputCol=output_col)

    return tokenizer.transform(dataframe)


def remove_stopwords(dataframe, input_col, output_col) -> DataFrame:
    remover = StopWordsRemover(inputCol=input_col, outputCol=output_col)
    return remover.transform(dataframe)
