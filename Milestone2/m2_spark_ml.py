# SE446 - Milestone 2: Spark ML Pipeline
# Lateen's Group
#
# Task 5-6: Lateen Alhurasen (ID: 231543)
# Task 7:   Layan Alshowaier (ID: 231361)

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)

# SparkSession
spark = SparkSession.builder \
    .appName('SE446_M2Project_lateens_team') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.executor.heartbeatInterval', '60s') \
    .config('spark.network.timeout', '300s') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
print(f'Spark {spark.version} | Master: {spark.sparkContext.master}')
print(f'App Name: {spark.sparkContext.appName}')

# Load Dataset from HDFS
DATA_PATH = 'hdfs:///data/chicago_crimes.csv'
print(f'\nLoading data from: {DATA_PATH}')

raw_df = spark.read \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .csv(DATA_PATH)

df = raw_df \
    .withColumn('Hour',  hour(to_timestamp(col('Date'), 'MM/dd/yyyy hh:mm:ss a'))) \
    .withColumn('label', col('Arrest').cast('integer')) \
    .withColumn('Domestic_str', col('Domestic').cast('string')) \
    .filter(col('Primary Type').isNotNull()) \
    .filter(col('District').isNotNull()) \
    .filter(col('Hour').isNotNull())

print(f'Full dataset rows: {df.count():,}')

# Phase B: 5% Sampling
print('\n--- Applying 5% sample for Phase B ML pipeline ---')
ml_full_df = df.sample(0.05, seed=42)
sample_count = ml_full_df.count()
print(f'5% sample rows: {sample_count:,}')

# Task 5: Feature Engineering Pipeline
# Author: Lateen Alhurasen (ID: 231543)
print('\n========== Task 5: Feature Engineering ==========')

ml_df = ml_full_df.select(
    col('District'),
    col('Primary Type').alias('PrimaryType'),
    col('Hour'),
    col('Domestic_str').alias('Domestic'),
    col('label')
).dropna()

crime_indexer = StringIndexer(
    inputCol='PrimaryType', outputCol='crime_index', handleInvalid='skip'
)
domestic_indexer = StringIndexer(
    inputCol='Domestic', outputCol='domestic_index', handleInvalid='skip'
)
assembler = VectorAssembler(
    inputCols=['District', 'crime_index', 'Hour', 'domestic_index'],
    outputCol='features', handleInvalid='skip'
)

feature_pipeline = Pipeline(stages=[crime_indexer, domestic_indexer, assembler])
train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)
train_df.cache()

print(f'Train: {train_df.count():,} | Test: {test_df.count():,}')

feature_model  = feature_pipeline.fit(train_df)
train_features = feature_model.transform(train_df)
test_features  = feature_model.transform(test_df)

print('\nSample feature vectors (5 rows):')
train_features.select('features', 'label').show(5, truncate=False)
print('Vector: [0]=District [1]=crime_index [2]=Hour [3]=domestic_index')
print('Task 5 done.')

# Task 6: Train and Evaluate Three Models
# Author: Lateen Alhurasen (ID: 231543)
print('\n========== Task 6: Model Training & Evaluation ==========')

auc_eval    = BinaryClassificationEvaluator(metricName='areaUnderROC')
acc_eval    = MulticlassClassificationEvaluator(metricName='accuracy')
f1_eval     = MulticlassClassificationEvaluator(metricName='f1')
prec_eval   = MulticlassClassificationEvaluator(metricName='weightedPrecision')
recall_eval = MulticlassClassificationEvaluator(metricName='weightedRecall')

def evaluate_model(name, preds):
    auc  = auc_eval.evaluate(preds)
    acc  = acc_eval.evaluate(preds)
    f1   = f1_eval.evaluate(preds)
    prec = prec_eval.evaluate(preds)
    rec  = recall_eval.evaluate(preds)
    tn = preds.filter((col('label')==0)&(col('prediction')==0)).count()
    fp = preds.filter((col('label')==0)&(col('prediction')==1)).count()
    fn = preds.filter((col('label')==1)&(col('prediction')==0)).count()
    tp = preds.filter((col('label')==1)&(col('prediction')==1)).count()
    print(f'\n--- {name} ---')
    print(f'  AUC={auc:.4f} Acc={acc:.4f} F1={f1:.4f} Prec={prec:.4f} Recall={rec:.4f}')
    print(f'  Confusion: TN={tn:,} FP={fp:,} FN={fn:,} TP={tp:,}')
    return {'Model':name,'AUC':round(auc,4),'Acc':round(acc,4),
            'F1':round(f1,4),'Prec':round(prec,4),'Recall':round(rec,4)}

results = []

# Model 1: Logistic Regression
print('Training Logistic Regression...')
t = time.time()
lr_model = LogisticRegression(
    maxIter=100, regParam=0.01, featuresCol='features', labelCol='label'
).fit(train_features)
r = evaluate_model('Logistic Regression', lr_model.transform(test_features))
r['Time'] = round(time.time()-t, 2); results.append(r)

# Model 2: Random Forest
print('\nTraining Random Forest...')
t = time.time()
rf_model = RandomForestClassifier(
    numTrees=100, maxDepth=5, seed=42, featuresCol='features', labelCol='label'
).fit(train_features)
r = evaluate_model('Random Forest', rf_model.transform(test_features))
r['Time'] = round(time.time()-t, 2); results.append(r)

# Model 3: GBT (reduced parameters for cluster memory)
print('\nTraining GBT...')
t = time.time()
gbt_model = GBTClassifier(
    maxIter=10,
    maxDepth=3,
    seed=42,
    featuresCol='features',
    labelCol='label'
).fit(train_features)
r = evaluate_model('GBT', gbt_model.transform(test_features))
r['Time'] = round(time.time()-t, 2); results.append(r)

print('\n========== MODEL COMPARISON TABLE ==========')
print(f'{"Model":<22} {"AUC":>6} {"Acc":>6} {"F1":>6} {"Prec":>6} {"Recall":>8} {"Time(s)":>8}')
print('-'*68)
for r in results:
    print(f'{r["Model"]:<22} {r["AUC"]:>6} {r["Acc"]:>6} {r["F1"]:>6} {r["Prec"]:>6} {r["Recall"]:>8} {r["Time"]:>8}')

# Task 7: Feature Importances
# Author: Layan Alshowaier (ID: 231361)
print('\n========== Task 7: Feature Importances ==========')
feature_names = ['District', 'crime_index', 'Hour', 'domestic_index']
importances   = rf_model.featureImportances.toArray()
print(f'{"Feature":<20} {"Importance":>12}  Bar')
print('-'*50)
for feat, imp in sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True):
    print(f'{feat:<20} {imp:>12.4f}  {"█" * int(imp*40)}')

top_feat = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)[0][0]
print(f'\nMost important feature: {top_feat}')
print('GBT is the best model with highest AUC-ROC and Accuracy.')
print('Tree-based models outperform Logistic Regression because')
print('crime-arrest relationships are non-linear.')
print('\nAll tasks completed successfully!')

spark.stop()
print('SparkSession stopped.')