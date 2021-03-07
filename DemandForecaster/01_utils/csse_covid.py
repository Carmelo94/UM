import pandas as pd

class CSSE_COVID:
  '''
  COVID data from John Hopkins Center for Systems Science and Engineering Github page https://github.com/CSSEGISandData.
  '''
  urls = {
      'gl_confirmed': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
      'gl_deaths'   : 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
      'us_confirmed': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv',
      'us_deaths'   : 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'
  }

  @staticmethod
  def split_global(global_results):
    '''
    Split global results by countries containing nulls in province/region column.
    '''
    # list matches col position
    expected_col_vals = ['Province/State', 'Country/Region']

    for i in range(len(expected_col_vals)):
      if global_results.columns[i] != expected_col_vals[i]:
        print(f"[COLUMN MISMATCH] Position: {i}, Source: {global_results.columns[i]}, Expected: {expected_col_vals[i]}")
        return None
      else:
        countries = global_results.iloc[:, :2].drop_duplicates()

        # group countries by the number of nulls in Province/State column
        null_count = countries['Province/State'].isnull().groupby(countries['Country/Region']).sum().reset_index(name='Nulls')

        # if Nulls=1, then we know that is the country total. Otherwise, if Nulls=0, then we need to group the country to get a true country total
        null_count_0 = list(null_count[null_count['Nulls']==0].iloc[:, 0])
        null_count_1 = list(null_count[null_count['Nulls']==1].iloc[:, 0])

        # countries containing no porvince/state nulls
        results_0 = global_results.copy()
        results_0 = results_0[results_0.iloc[:, 1].isin(null_count_0)]
        results_0.fillna(0, inplace=True)

        results_1 = global_results.copy()
        results_1 = results_1[results_1.iloc[:, 1].isin(null_count_1)]
        results_1.fillna(0, inplace=True)

        # replace Country/Region with Province/State if Province/State is not 0
        results_1['Country/Region'] = results_1.apply(lambda x: x['Country/Region'] if x['Province/State']==0 else x['Province/State'], axis=1)

        return [results_0, results_1]

  @staticmethod
  def calc_DoD(calculate, df):
    '''
    Calculate daily covid metric value.
    '''
    if calculate:
      metrics = ['confirmed', 'deaths']
      for m in metrics:
        if 'state' in df.columns:
          df[f"{m}_DoD"] = df.groupby('state')[m].transform(lambda x: x.diff())
        else:
          df[f"{m}_DoD"] = df.groupby('market')[m].transform(lambda x: x.diff())

    return df


  def gl_confirmed(self):
    '''
    Get global confirmed COVID cases: https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv
    '''
    url = self.urls['gl_confirmed']
    results = pd.read_csv(url)

    # combine split results
    df = pd.concat(self.split_global(results))

    # identify position of last dimension
    id_vars = df.columns[:list(df.columns).index('1/22/20')]
    df = df.melt(id_vars=id_vars)

    # group data by country and date
    df = df.pivot_table(index=['Country/Region', 'variable'], values='value', aggfunc='sum').reset_index()
    df['variable'] = pd.to_datetime(df['variable'])
    df.sort_values(by=['Country/Region', 'variable'], inplace=True)

    df.rename(columns={'variable':'date', 'Country/Region':'market'}, inplace=True)
    df['metric'] = 'confirmed'

    return df

  def gl_deaths(self):
    '''
    Get global death COVID cases: https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_deaths.csv
    '''
    url = self.urls['gl_deaths']
    results = pd.read_csv(url)

    # # combine split results
    df = pd.concat(self.split_global(results))

    # identify position of last dimension
    id_vars = df.columns[:list(df.columns).index('1/22/20')]
    df = df.melt(id_vars=id_vars)

    # group data by country and date
    df = df.pivot_table(index=['Country/Region', 'variable'], values='value', aggfunc='sum').reset_index()
    df['variable'] = pd.to_datetime(df['variable'])
    df.sort_values(by=['Country/Region', 'variable'], inplace=True)

    df.rename(columns={'variable':'date', 'Country/Region':'market'}, inplace=True)
    df['metric'] = 'deaths'

    return df

  def us_confirmed(self):
    '''
    Get us confirmed COVID cases: https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv
    '''
    url = self.urls['us_confirmed']
    results = pd.read_csv(url)
    results.fillna(0, inplace=True)

    # identify position of last dimension
    id_vars = results.columns[:list(results.columns).index('1/22/20')]
    df = results.melt(id_vars=id_vars)

    # split US data by state and total us
    # Total US
    total_us_index = [c for c in df.columns if 'country' in c.lower()] + ['variable']
    df_total_us = df.pivot_table(index=total_us_index, values='value', aggfunc='sum').reset_index()
    df_total_us['variable'] = pd.to_datetime(df_total_us['variable'])
    df_total_us.sort_values(by=total_us_index, inplace=True)

    df_total_us.rename(columns={**{c:'market' for c in df_total_us.columns if 'country' in c.lower()}, **{'variable':'date'}}, inplace=True)
    df_total_us['metric'] = 'confirmed'

    # State
    state_index = [c for c in df.columns if 'country' in c.lower() or 'state' in c.lower()] + ['variable']
    df_state = df.pivot_table(index=state_index, values='value', aggfunc='sum').reset_index()
    df_state['variable'] = pd.to_datetime(df_state['variable'])
    df_state.sort_values(by=state_index, inplace=True)

    df_state.rename(columns={**{c:'market' for c in df_state.columns if 'country' in c.lower()}, **{c:'state' for c in df_state.columns if 'state' in c.lower()}, **{'variable':'date'}}, inplace=True)
    df_state['metric'] = 'confirmed'

    return {'total_us':df_total_us, 'state':df_state}

  def us_deaths(self):
    '''
    Get us deaths COVID cases: https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv
    '''
    url = self.urls['us_deaths']
    results = pd.read_csv(url)
    results.fillna(0, inplace=True)

    # identify position of last dimension
    id_vars = results.columns[:list(results.columns).index('1/22/20')]
    df = results.melt(id_vars=id_vars)

    # split US data by state and total us
    # Total US
    total_us_index = [c for c in df.columns if 'country' in c.lower()] + ['variable']
    df_total_us = df.pivot_table(index=total_us_index, values='value', aggfunc='sum').reset_index()
    df_total_us['variable'] = pd.to_datetime(df_total_us['variable'])
    df_total_us.sort_values(by=total_us_index, inplace=True)

    df_total_us.rename(columns={**{c:'market' for c in df_total_us.columns if 'country' in c.lower()}, **{'variable':'date'}}, inplace=True)
    df_total_us['metric'] = 'deaths'

    # State
    state_index = [c for c in df.columns if 'country' in c.lower() or 'state' in c.lower()] + ['variable']
    df_state = df.pivot_table(index=state_index, values='value', aggfunc='sum').reset_index()
    df_state['variable'] = pd.to_datetime(df_state['variable'])
    df_state.sort_values(by=state_index, inplace=True)

    df_state.rename(columns={**{c:'market' for c in df_state.columns if 'country' in c.lower()}, **{c:'state' for c in df_state.columns if 'state' in c.lower()}, **{'variable':'date'}}, inplace=True)
    df_state['metric'] = 'deaths'

    return {'total_us':df_total_us, 'state':df_state}

  def get_data(self, global_=True, calc_daily_metric=False):
    '''
    Get aggregated COVID data.
        Parameters:
            global_(bool): True for data by country; False for US state data.
    '''
    df_total_us_confirmed = self.us_confirmed()
    df_total_us_deaths = self.us_deaths()    

    if global_:
      df_global_confirmed = self.gl_confirmed()
      df_global_deaths = self.gl_deaths()
      df_global = pd.concat([df_total_us_confirmed['total_us'], df_total_us_deaths['total_us'], df_global_confirmed, df_global_deaths])
      df_pvt = df_global.pivot_table(index=['market', 'date'], columns='metric', values='value', aggfunc='sum').reset_index()
      return self.calc_DoD(calc_daily_metric, df_pvt)

    else:
      df_us = pd.concat([df_total_us_confirmed['state'], df_total_us_deaths['state']])
      df_pvt = df_us.pivot_table(index=['market', 'state', 'date'], columns='metric', values='value', aggfunc='sum').reset_index()
      return self.calc_DoD(calc_daily_metric, df_pvt)