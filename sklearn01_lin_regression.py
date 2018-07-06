from datetime import timedelta

import matplotlib as mpl; mpl.use('TkAgg')
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import median_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import LabelBinarizer
from sklearn_pandas import DataFrameMapper, gen_features


if __name__ == '__main__':
    # Process raw data.
    df = pd.read_csv('~/data/sample.csv')
    cols = ['FlightNum', 'FlightDate', 'DayOfWeek', 'DayofMonth', 'Carrier', 'Origin',
            'Dest', 'Distance', 'DepDelay', 'ArrDelay', 'CRSDepTime', 'CRSArrTime']
    df = df[cols]
    df = df.rename(columns={'DayofMonth':'DayOfMonth'})
    df['DayOfYear'] = df.DayOfMonth.map(str) + '-' + df.DayOfWeek.map(str)
    dep_time = pd.to_datetime(df.FlightDate.map(str) +
        df.CRSDepTime.map(str).apply(lambda x: x.zfill(4)), format='%Y-%m-%d%H%M')
    arr_time = pd.to_datetime(df.FlightDate.map(str) +
        df.CRSArrTime.map(str).apply(lambda x: x.zfill(4)), format='%Y-%m-%d%H%M')
    times = pd.concat([dep_time, arr_time], axis=1, keys=['dep', 'arr'])
    times.loc[times.arr <= times.dep, 'arr'] = times.arr.apply(lambda x: x + timedelta(1))
    df['FlightDate'] = pd.to_datetime(df.FlightDate)
    df['DayOfYear'] = df.FlightDate.apply(lambda x: x.timetuple().tm_yday)
    df['CRSDepTime'] = times.dep
    df['CRSArrTime'] = times.arr
    df = df[~df.isnull().any(axis=1)]
    df = df.sort_values(['DayOfYear', 'Carrier', 'Origin', 'Dest', 'FlightNum',
                         'CRSDepTime', 'CRSArrTime'], inplace=False).reset_index(drop=True)
    # Select subset
    training = df[['FlightNum', 'DayOfWeek', 'DayOfMonth', 'Carrier', 'Origin', 'Dest',
                   'Distance', 'DepDelay', 'CRSDepTime', 'CRSArrTime', 'DayOfYear']]
    training.loc[:, ['CRSDepTime', 'CRSArrTime']] = training.loc[:, ['CRSDepTime',
        'CRSArrTime']].astype(int)
    # Convert categoricals to indicators.
    feature_def = gen_features(
        columns=['Carrier', 'Origin', 'Dest'],
        classes=[LabelBinarizer]
    )
    mapper = DataFrameMapper(feature_def, default=None)
    training_vectors = mapper.fit_transform(training)
    results_vector = df.ArrDelay.values
    df_training = pd.DataFrame(columns=mapper.transformed_names_,
                               data=training_vectors)
    # Generate train/test sets.
    X_train, X_test, y_train, y_test = train_test_split(
      training_vectors,
      results_vector,
      test_size=0.1,
      random_state=43
    )
    # Do the regression.
    regressor = LinearRegression()
    regressor.fit(X_train, y_train)
    predicted = regressor.predict(X_test)
    # Show results
    medae = median_absolute_error(y_test, predicted)
    r2 = r2_score(y_test, predicted)
    print("Median absolute error:    {:.3g}".format(medae))
    print("r2 score:                 {:.3g}".format(r2))
    df_results = pd.DataFrame({'actual':y_test, 'predicted':predicted})
    df_results.plot(kind='scatter', x='actual', y='predicted')
    plt.show()
