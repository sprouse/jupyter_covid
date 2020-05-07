# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
from IPython.core.display import display, HTML, Markdown
from bokeh.plotting import figure, output_file, show
import dask.dataframe as dd
from datetime import date
import matplotlib.pyplot as plt
import pandas as pd
import dateparser
import seaborn as sns
from tqdm import tqdm


pd.set_option('display.max_rows', 100)
# This line enables formatted table output in nbconvert to PDF
pd.options.display.latex.repr = False

for_pdf = True
if not for_pdf:
    display(HTML("<style>.container { width:100% !important; }</style>"))
# -

# %matplotlib inline

df_states = pd.read_csv("covid-19-data/us-states.csv")
df_states.rename(columns={"cases": "total_cases"}, inplace=True)
df_states.sort_values("date").reset_index(drop=True, inplace=True)
df_counties = pd.read_csv("covid-19-data/us-counties.csv")
df_counties.rename(columns={"cases": "total_cases"}, inplace=True)

df_counties.loc[(df_counties.county == 'New York City') &
                (df_counties.state == 'New York'), 'fips'] = 36061

# Read population data
df_population = pd.read_csv("co-est2019-alldata.csv.gz", encoding = "ISO-8859-1" )
df_population['fips'] = df_population.apply(lambda x: f"{x.STATE}{x.COUNTY:03}", axis=1)
fips_to_pop = dict(zip(df_population.fips.astype(int), df_population.POPESTIMATE2019))


def calc(df):
    df['new_cases'] = df.groupby('fips')['total_cases'].diff()
    df['new_deaths'] = df.groupby('fips')['deaths'].diff()


calc(df_counties)
calc(df_states)

fips_to_state= {k:v for d in list(df_states.groupby(["state"]).first().apply(lambda x: {x.fips: x.name}, axis=1)) for k, v in d.items()}
state_to_fips= {k:v for d in list(df_states.groupby(["state"]).first().apply(lambda x: {x.name: x.fips}, axis=1)) for k, v in d.items()}

fips_to_county= {k:v for d in list(df_counties.groupby(["county", "state"]).first().apply(lambda x: {x.fips: x.name}, axis=1)) for k, v in d.items()}
county_to_fips= {k:v for d in list(df_counties.groupby(["county", "state"]).first().apply(lambda x: {x.name: x.fips}, axis=1)) for k, v in d.items()}

df_counties['pop'] = df_counties.fips.map(fips_to_pop)

df_counties['county_state'] = df_counties.apply(lambda x: f"{x.county}, {x.state}", axis=1)

df_counties['zombies'] = df_counties.total_cases / df_counties['pop'] * 100000
df_counties['new_zombies'] = df_counties.new_cases / df_counties['pop'] * 100000
df_counties['new_zombies'] = df_counties.sort_values('date').groupby('fips')['new_zombies'].apply(lambda x: x.rolling(7).mean())

df_counties.query("county=='Santa Clara' and state=='California'").tail(5)


# # Overview

# The New York Times publishes their Covid-19 dataset daily to github here: `https://github.com/nytimes/covid-19-data.git`
#
# This notebook was inspired by the Minute Physics YouTube video: https://www.youtube.com/watch?v=54XLXg4fYsc
#
# I wanted to recreate at the local county level the plots that are shown on this site: https://aatishb.com/covidtrends/

# # Plots

# Aatish B's log log plot
def ll_plot(df, geos, xlim=None, max_pop=None):
    df = df.sort_values('total_cases')
    fig = plt.figure(figsize=(16, 6))
    ax = plt.gca()
    for geo in geos:
        if isinstance(geo, tuple):
            fip = county_to_fips[geo]
            label = f"{geo[0]}, {geo[1]}"
        else:
            fip = state_to_fips[geo]
            label = geo
        df.query(f"fips=={fip} and new_cases>0").plot(
            x='total_cases', y='new_cases', label=label, ax=ax)
    plt.yscale("log")
    plt.xscale("log")
    plt.ylabel("Daily New Cases")
    if xlim:
        plt.xlim(1, xlim)
    if max_pop:
        ax.plot([max_pop[1], max_pop[1]], [1, 1e6],
                color="red", linestyle="--")
        ax.text(max_pop[1]*.75, 1e1, f"{max_pop[0]} Population", rotation=90)


# + hide_input=false
def decade_time(county, state, end_value=None):
    df=df_counties
    state = state.replace("'", "\\'")
    county = county.replace("'", "\\'")

    if not end_value:
        end_value = df.query(f"state=='{state}' and county=='{county}'").total_cases.max()
    try:
        x = df.query(f"state=='{state}' and county=='{county}' and total_cases<{end_value}").iloc[-1].date
        y = df.query(f"state=='{state}' and county=='{county}' and total_cases<{end_value/10}").iloc[-1].date
        xd = dateparser.parse(x)
        yd = dateparser.parse(y)
        delta = xd - yd
    except IndexError:
        return None
    return int(delta.days)


# -

def decade_time_group(g):
    df = g.sort_values('date')
    end_value = df.total_cases.max()
    try:
        x = df.query(f"total_cases<{end_value}").iloc[-1].date
        y = df.query(f"total_cases<{end_value/10}").iloc[-1].date
        xd = dateparser.parse(x)
        yd = dateparser.parse(y)
        delta = xd - yd
    except IndexError:
        return None
    return int(delta.days)


def decade_time2(part, row):
    #df=df_counties
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


def decade_time_state(state, end_value=None):
    df=df_states
    # state = state.replace("'", "\\'")

    if not end_value:
        end_value = df.query(f"state=='{state}'").total_cases.max()
    try:
        x = df.query(f"state=='{state}' and total_cases<{end_value}").iloc[-1].date
        y = df.query(f"state=='{state}' and total_cases<{end_value/10}").iloc[-1].date
        xd = dateparser.parse(x)
        yd = dateparser.parse(y)
        delta = xd - yd
    except IndexError:
        return None
    return delta.days


def decade_time_state2(part, row):
    df=part.sort_index()
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


# The `log-log` plot does not show time and one might wonder how long does it take for the total cases to increase by a factor of 10x. (I called this " _decade time_ " ).
# In each county, for each day compute how many days have elapsed since the total number of cases was 1/10 the current total number of cases.
#
# Social distancing should be slowing the rate at which the total cases are growing.
# We calculate the _decade time_ for every day and for every county.
#
# **Note that this calculation may take more than 5 minutes to complete**

state_list = ['New York', 'California', 'Colorado', 'Michigan', 'Iowa', 'Maine', 'Florida']
df_counties_dd = dd.from_pandas(df_counties.query("state in @state_list"), npartitions=10)
df_states_dd = dd.from_pandas(df_states, npartitions=10)


def mapper(part):
    #if part.iloc[0].county == 'Santa Clara':
        #print(part)
    part['decade_time'] = part.apply(
        lambda row: decade_time2(part, row), axis=1)
    return part


def mapper_state(part):
    #if part.iloc[0].county == 'Santa Clara':
        #print(part)
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

df_states = df2

# ## State Plots

ll_plot(df_states.query("total_cases>50"), geos=[
    'California',
    'New York',
    'Washington',
    'Colorado',
    "Michigan",
    "Texas"
])

ll_plot(df_counties.query("total_cases>50 and new_cases>0"), geos=[
    ('Santa Clara','California'),
    ('Los Angeles', 'California'),
    ('Suffolk', 'New York'),
    ('Boulder', 'Colorado'),
    ('Kent', "Michigan"),
])


def plot_county(county, state):
    fips = county_to_fips[(county, state)]
    pop = fips_to_pop[fips]
    display(Markdown(f"## {county}, {state}"))
    ll_plot(df_counties.query("total_cases > 50"),
           geos=[(county, state)],
           max_pop=(county, pop)
           )
    display(Markdown(f"{county}'s current decade time is {decade_time(county, state)} days"))
    display(Markdown(f"{county}'s population {pop/1e6:.2f} million"))


# Aatish B's log log plot
def ll_plot2(x, y, label, xlim=None):
    fig = plt.figure(figsize=(16, 6))
    ax = plt.gca()

    plt.plot(x, y, label=label)
    plt.yscale("log")
    plt.xscale("log")
    plt.ylabel("Daily New Cases")
    plt.xlim(.1, 100000)
    plt.ylim(.0001, 10000)


def plot_county_norm(county, state):
    fips = county_to_fips[(county, state)]
    pop = fips_to_pop[fips]
    display(Markdown(f"## {county}, {state}"))
    dft = df_counties.query(f"state=='{state}' and county=='{county}' and new_zombies > 0").sort_values("date")
    ll_plot2(dft.zombies,
            dft.new_zombies,
           label=f"{county}, {state}",
           )
    display(Markdown(f"{county}'s current decade time is {decade_time(county, state)} days"))
    display(Markdown(f"{county}'s population {pop/1e6:.2f} million"))
    plt.show()


for county, state in [
    ('Santa Clara', 'California'),
    ('Los Angeles', 'California'),
    ('Suffolk', 'New York'),
    ('New York City', 'New York'),
    ('Boulder', 'Colorado'),
    ('Kent', "Michigan"),
]:
    plot_county_norm(county, state)

df_counties.query("state=='California' and county=='Santa Clara'").tail(5)

plot_county("New York City", "New York")

plot_county('Boulder', 'Colorado')

plot_county('Kent', 'Michigan')

plot_county('Santa Clara', 'California')

plot_county('Los Angeles', 'California')

plot_county('Suffolk', 'New York')

plot_county('Sagadahoc', 'Maine')


# # Decade Times
# ## County Level

def show_decade(county, state, tail=5):
    _cols=['date', 'total_cases','new_cases',  'deaths', 'new_deaths', 'pop', 'decade_time', 'zombies', 'new_zombies', 'fips']
    display(Markdown(f"### {county} {state} Decade Times (Past Two Weeks)\n Zombies = total_cases/100,000 people"))
    display(df_counties.query(f"state=='{state}' and county=='{county}'").tail(tail)[_cols])


for county, state in [
    ('Boulder', 'Colorado'),
    ('Kent', 'Michigan'),
    ('Santa Clara', 'California'),
    ('Los Angeles', 'California'),
    ('Suffolk', 'New York'),
    ('Sagadahoc', 'Maine'),
    ('Tompkins', 'New York'),
    ('Duval', 'Florida'),
    ('Polk', 'Iowa')
]:
    show_decade(county, state)

show_decade('Santa Clara', 'California', tail=14)

show_decade('Los Angeles', 'California', tail=14)

show_decade('Suffolk', 'New York')

show_decade('Sagadahoc', 'Maine')

show_decade('Tompkins', 'New York')

df_counties.groupby('fips').last().tail(5)

display(Markdown(f"### 20 Worst Counties"))
df_counties.groupby('fips').last().query("decade_time>0").sort_values(
    'new_zombies',
    ascending=False
).reset_index(drop=True
              ).head(20)[['date', 'county', 'state', 'total_cases', 'new_cases',
                          'pop', 'zombies', 'new_zombies', 'decade_time']]

display(Markdown(f"### 10 Best Counties"))
df_counties.groupby('fips').last().query("decade_time>0").sort_values(
    'zombies',
    ascending=True
).reset_index(drop=True
              ).head(10)[['date', 'county', 'state', 'total_cases', 'pop',
                          'zombies', 'new_zombies', 'decade_time']]

# ## County Decade Plots

counties = [
    ('Boulder', 'Colorado'),
    ('Santa Clara', 'California'),
    ('Suffolk', 'New York'),
    ('Kent', 'Michigan'),
    ('Los Angeles', 'California'),
    ('Tompkins', 'New York')
]


def decade_plot(counties):
    plt.figure(figsize=(16, 6))
    fips = [county_to_fips[x] for x in counties]
    df = df_counties[df_counties.fips.isin(fips)].dropna(
        subset=['decade_time'], axis=0).sort_values('date')
    df['decade_time'] = df['decade_time'].astype(int)
    g = sns.lineplot(x='date',
                     y='decade_time',
                     hue='county',
                     data=df
                     )
    x = plt.xticks(rotation=55)
    for i, label in enumerate(g.xaxis.get_ticklabels()):
        if i % 5 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)


decade_plot(counties)

df_counties.dtypes



# ## State Level
# ### State Decade Plot

def decade_plot_states(states):
    df = df_states[df_states.state.isin(states)].dropna(
        subset=['decade_time'], axis=0).sort_values('date')
    df['decade_time'] = df['decade_time'].astype(int)
    plt.figure(figsize = (16, 6))
    g=sns.lineplot(x = 'date',
                     y = 'decade_time',
                     hue = 'state',
                     data = df,
                     )
    x=plt.xticks(rotation = 55)
    for i, label in enumerate(g.xaxis.get_ticklabels()):
        if i % 5 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)


decade_plot_states([
    'New York',
    'California',
    'Colorado',
    #'Texas',
    #'Washington',
    'Maine',
    'Michigan'
])

display(Markdown("### 10 Worst States"))
df_states.sort_values('date').groupby('state').last().sort_values('decade_time').head(10)

display(Markdown("### 10 Best States"))
df_states.sort_values('date').groupby('state').last().sort_values(
    'decade_time', ascending=False).head(10)

df_counties.query("state=='New York'").sort_values('date').groupby('fips').last().sort_values(
    'total_cases', ascending=False).head(30)

dft = df_counties.groupby("fips").last()

dft['days_to_all'] = dft['pop']/dft['new_cases']

dft.query("new_cases > 1").sort_values('days_to_all', ascending=True).head(20)

# ## Sandbox

clist = ['Santa Clara, California',
         'Los Angeles, California',
         #'Orange, California',
         'New York City, New York',
         'Suffolk, New York',
         'Cumberland, Maine',
         'Kent, Michigan',
         'Boulder, Colorado',
         'Larimer, Colorado',
         'Polk, Iowa',
         #'Miami-Dade, Florida',
         ]

from bokeh.plotting import output_notebook, reset_output, save
from bokeh.models.tools import HoverTool
from bokeh.palettes import Category20_12 as palette
reset_output()
output_notebook()

today = date.today()
today_date = today.strftime("%b-%d-%Y")

# output to static HTML file
p = figure(plot_width=1200,
           x_axis_type="log",
           y_axis_type="log",
           x_range=(10, 15000),
           aspect_ratio=2/1,
           active_drag="box_zoom",
           title=f"NY Times Covid Dataset - Normalized per 100K Population (Updated {today_date})"
           )
# add a circle renderer with a size, color, and alpha
for i, sc in enumerate(clist):
    print(i, sc)
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

# +
hover = HoverTool()
hover.tooltips=[
    ('Date', '@date'),
    ('County', '@county_state'),
    ('Cases/100K', '@zombies{0.0}'),
    ('New Cases/100K', '@new_zombies{0.0}'),
]

p.add_tools(hover)
# -

# show the results
show(p)

save(p, "covid.html", title="Covid Plot")

df_counties.query("state=='Florida'").sort_values("new_zombies").groupby('county').last().sort_values(
    'new_zombies', ascending=False)




