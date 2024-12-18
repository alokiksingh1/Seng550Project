from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

def create_feature_pipeline(input_cols, output_col):
    #Create a feature engineering pipeline.
    stages = []
    for col in input_cols:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index")
        encoder = OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_vec")
        stages += [indexer, encoder]
    assembler = VectorAssembler(inputCols=[f"{col}_vec" for col in input_cols], outputCol=output_col)
    stages.append(assembler)
    return Pipeline(stages=stages)
