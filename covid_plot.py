from bokeh.models.tools import HoverTool
from bokeh.palettes import Category20_12 as palette
from bokeh.plotting import figure, output_file, save
import dask.dataframe as dd
from datetime import date
import pandas as pd
import dateparser

pd.set_option('display.max_rows', 100)
# This line enables formatted table output in nbconvert to PDF
pd.options.display.latex.repr = False

df_states = pd.read_csv("covid-19-data/us-states.csv")
df_states.rename(columns={"cases": "total_cases"}, inplace=True)
df_states.sort_values("date").reset_index(drop=True, inplace=True)
df_counties = pd.read_csv("covid-19-data/us-counties.csv")
df_counties.rename(columns={"cases": "total_cases"}, inplace=True)

df_counties.loc[(df_counties.county == 'New York City') &
                (df_counties.state == 'New York'), 'fips'] = 36061

# Read population data
df_population = pd.read_csv(
    "co-est2019-alldata.csv.gz", encoding="ISO-8859-1")
df_population['fips'] = df_population.apply(
    lambda x: f"{x.STATE}{x.COUNTY:03}", axis=1)
fips_to_pop = dict(zip(
    df_population.fips.astype(int), df_population.POPESTIMATE2019))


def calc(df):
    df['new_cases'] = df.groupby('fips')['total_cases'].diff()
    df['new_deaths'] = df.groupby('fips')['deaths'].diff()


calc(df_counties)
calc(df_states)

fips_to_state = {k: v for d in list(
    df_states.groupby(["state"]).first().apply(
        lambda x: {x.fips: x.name}, axis=1)) for k, v in d.items()}
state_to_fips = {k: v for d in list(
    df_states.groupby(["state"]).first().apply(
        lambda x: {x.name: x.fips}, axis=1)) for k, v in d.items()}

fips_to_county = {k: v for d in list(
    df_counties.groupby(["county", "state"]).first().apply(
        lambda x: {x.fips: x.name}, axis=1)) for k, v in d.items()}
county_to_fips = {k: v for d in list(
    df_counties.groupby(["county", "state"]).first().apply(
        lambda x: {x.name: x.fips}, axis=1)) for k, v in d.items()}

df_counties['pop'] = df_counties.fips.map(fips_to_pop)

df_counties['county_state'] = df_counties.apply(
    lambda x: f"{x.county}, {x.state}", axis=1)

df_counties['zombies'] = df_counties.total_cases / df_counties['pop'] * 100000
df_counties['new_zombies'] = (df_counties.new_cases / df_counties['pop']
                              * 100000)
df_counties['new_zombies'] = df_counties.sort_values('date').groupby(
    'fips')['new_zombies'].apply(lambda x: x.rolling(7).mean())

df_counties.query("county=='Santa Clara' and state=='California'").tail(5)


def decade_time_state(state, end_value=None):
    df = df_states

    if not end_value:
        end_value = df.query(f"state=='{state}'").total_cases.max()
    try:
        x = df.query(
            f"state=='{state}' and total_cases<{end_value}").iloc[-1].date
        y = df.query(
            f"state=='{state}' and total_cases<{end_value/10}").iloc[-1].date
        xd = dateparser.parse(x)
        yd = dateparser.parse(y)
        delta = xd - yd
    except IndexError:
        return None
    return delta.days


def decade_time_state2(part, row):
    df = part.sort_index()
    # state = state.replace("'", "\\'")

    end_value = row.total_cases
    try:
        x = df.query(f"total_cases<{end_value}").iloc[-1].date
        y = df.query(f"total_cases<{end_value/10}").iloc[-1].date
        xd = dateparser.parse(x)
        yd = dateparser.parse(y)
        delta = xd - yd
    except IndexError:
        return None
    return delta.days


# The `log-log` plot does not show time and one might wonder how long does
# it take for the total cases to increase by a factor of 10x.
# (I called this " _decade time_ " ).
# In each county, for each day compute how many days have elapsed since the
# total number of cases was 1/10 the current total number of cases.
#
# Social distancing should be slowing the rate at which the total cases
# are growing.
# We calculate the _decade time_ for every day and for every county.
#
# **Note that this calculation may take more than 5 minutes to complete**

state_list = [
    'New York',
    'California',
    'Colorado',
    'Michigan',
    'Iowa',
    'Maine',
    'Florida'
]
df_counties_dd = dd.from_pandas(
    df_counties.query("state in @state_list"), npartitions=10)
df_states_dd = dd.from_pandas(
    df_states, npartitions=10)


def decade_time2(part, row):
    df = part.sort_index()

    end_value = row.total_cases
    try:
        x = df.query(f"total_cases<{end_value}").iloc[-1].date
        y = df.query(f"total_cases<{end_value/10}").iloc[-1].date
        xd = dateparser.parse(x)
        yd = dateparser.parse(y)
        delta = xd - yd
    except IndexError:
        return None
    return delta.days


def mapper(part):
    part['decade_time'] = part.apply(
        lambda row: decade_time2(part, row), axis=1)
    return part


def mapper_state(part):
    part['decade_time'] = part.apply(
        lambda row: decade_time_state2(part, row), axis=1)
    return part


df_counties_dd.state

meta = {
    'date': 'str',
    'county': 'str',
    'state': 'str',
    'fips': 'f8',
    'total_cases': 'f8',
    'deaths': 'f8',
    'new_cases': 'f8',
    'new_deaths': 'f8',
    'pop': 'f8',
    'county_state': 'str',
    'zombies': 'f8',
    'new_zombies': 'f8',
    'decade_time': 'f8',
}
df1 = df_counties_dd.groupby('fips').apply(
    mapper,  meta=meta).compute(scheduler='processes')

df_counties = df1

df_states_dd.columns

meta = {
    'date': 'str',
    'state': 'str',
    'fips': 'f8',
    'total_cases': 'f8',
    'deaths': 'f8',
    'new_cases': 'f8',
    'new_deaths': 'f8',
    'decade_time': 'f8',
}
df2 = df_states_dd.groupby('fips').apply(
    mapper_state,  meta=meta).compute(scheduler='processes')


clist = ['Santa Clara, California',
         'Los Angeles, California',
         # 'Orange, California',
         'New York City, New York',
         'Suffolk, New York',
         'Cumberland, Maine',
         'Kent, Michigan',
         'Boulder, Colorado',
         'Larimer, Colorado',
         'Polk, Iowa',
         # 'Miami-Dade, Florida',
         ]


today = date.today()
today_date = today.strftime("%b-%d-%Y")

# output to static HTML file
p = figure(plot_width=1200,
           x_axis_type="log",
           y_axis_type="log",
           x_range=(10, 15000),
           aspect_ratio=2/1,
           active_drag="box_zoom",
           title=f"NY Times Covid Dataset"
           "- Normalized per 100K Population (Updated {today_date})"
           )
# add a circle renderer with a size, color, and alpha
for i, sc in enumerate(clist):
    # print(i, sc)
    p.circle(x='zombies',
             y='new_zombies',
             legend_group='county_state',
             size=10,
             color=palette[i],
             alpha=0.5,
             source=df_counties.query(f"county_state=='{sc}'"))
p.legend.location = "bottom_right"
p.legend.click_policy = "hide"
p.xaxis.axis_label = "Total Cases/100K Population"
p.yaxis.axis_label = "New Cases/100K"

hover = HoverTool()
hover.tooltips = [
    ('Date', '@date'),
    ('County', '@county_state'),
    ('Cases/100K', '@zombies{0.0}'),
    ('New Cases/100K', '@new_zombies{0.0}'),
]

p.add_tools(hover)

fn = "covid.html"
output_file(filename=fn, title="Covid Plot")
save(p, filename=fn, title="Covid Plot")
