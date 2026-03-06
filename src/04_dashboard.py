from __future__ import annotations

import sqlite3
import pandas as pd

from dash import Dash, dcc, html, dash_table, Input, Output
import plotly.express as px
from utils import DB_DIR

DB_PATH = DB_DIR / "beer_awards.sqlite"

def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

def load_filter_options() -> tuple[list[dict], list[dict]]:
    """return options for dropdown with medal"""
    with get_conn() as conn:
        years = pd.read_sql_query(
            "SELECT DISTINCT year FROM awards WHERE year IS NOT NULL ORDER BY year DESC;",
            conn,  
        )["year"].dropna().astype(int).tolist()

    year_options = [{"label": "All", "value": "ALL"}] + [
        {"label": str(y),"value": int(y)} for y in years
    ]
    medal_options = [{"label": "All", "value": "ALL"}] +[
        {"label": "Gold", "value": "Gold"},
        {"label": "Silver", "value": "Silver"},
        {"label": "Bronze", "value": "Bronze"},
    ]

    return year_options, medal_options

def build_where(year_val, medal_val) -> tuple[str,list]:
    where = []
    params = []

    where.append("a.brewery_name IS NOT NULL")
    where.append("TRIM(a.brewery_name) <> ''")

    if year_val and year_val != "ALL":
        where.append("a.year = ?")
        params.append(int(year_val))

    if medal_val and medal_val != "ALL":
        where.append("a.medal = ?")
        params.append(str(medal_val))

    where_sql = "WHERE " + " AND ".join(where)

    return where_sql, params

def kpis(year_val, medal_val) -> dict:
    where_sql, params = build_where(year_val, medal_val)
    q=f"""
    WITH brewery_country AS (
        SELECT 
            brewery_name_norm,
            MAX(country) AS country
        FROM breweries
        WHERE brewery_name_norm IS NOT NULL
        GROUP BY brewery_name_norm
    )
    SELECT
        COUNT(*) AS medals_total,
        COUNT(DISTINCT a.brewery_name_norm) AS breweries_total,
        COUNT(DISTINCT a.category) AS categories_total,
        SUM(
            CASE
                WHEN bc.country IS NOT NULL AND TRIM(bc.country) <> '' THEN 1
                ELSE 0
            END
        ) AS medals_matched
    FROM awards a
    LEFT JOIN brewery_country bc
        ON a.brewery_name_norm = bc.brewery_name_norm
    {where_sql}
    ;
    """
    
    with get_conn() as conn:
        row = pd.read_sql_query(q, conn, params=params).iloc[0].to_dict()

    medals_total = int(row.get("medals_total") or 0)
    medals_matched = int(row.get("medals_matched") or 0)
    match_rate = (medals_matched / medals_total) if medals_total else 0.0
    return {
        "medals_total": medals_total,
        "breweries_total": int(row.get("breweries_total") or 0),
        "categories_total": int(row.get("categories_total") or 0),
        "medals_matched": medals_matched,
        "match_rate": match_rate,
    }



def top_breweries(year_val, medal_val, top_n: int = 15) -> pd.DataFrame:
    where_sql, params = build_where(year_val, medal_val)
    q = f"""
    SELECT
        a.brewery_name,
        COUNT(*) AS medals_total,
        SUM(CASE WHEN a.medal='Gold' THEN 1 ELSE 0 END) AS golds,
        SUM(CASE WHEN a.medal='Silver' THEN 1 ELSE 0 END) AS silvers,
        SUM(CASE WHEN a.medal='Bronze' THEN 1 ELSE 0 END) AS bronzes
    FROM awards a
    {where_sql}
    GROUP BY a.brewery_name
    ORDER BY medals_total DESC, golds DESC, silvers DESC, bronzes DESC, a.brewery_name ASC
    LIMIT ?;
    """
    with get_conn() as conn:
        df = pd.read_sql_query(q, conn, params=params + [int(top_n)])
    return df

def medals_by_year( medal_val) -> pd.DataFrame:
    where = ["a.brewery_name IS NOT NULL", "TRIM(a.brewery_name) <> ''", "a.year IS NOT NULL"]
    params = []
    if medal_val and medal_val != "ALL":
        where.append("a.medal =?")
        params.append(str(medal_val))
    where_sql = "WHERE " + " AND ".join(where)

    q = f"""
    SELECT 
        a.year, 
        COUNT(*) AS medals_total
    FROM awards a
    {where_sql}
    GROUP BY a.year
    ORDER BY a.year;
    """
    with get_conn() as conn:
        df = pd.read_sql_query(q, conn, params=params)
    return df

def add_condition(where_sql: str, condition: str) -> str:
    if where_sql and where_sql.strip():
        return where_sql + " AND " + condition
    return "WHERE " + condition

def medals_by_country(year_val, medal_val, top_n: int = 15, include_unknown=False) -> pd.DataFrame:
    where_sql, params = build_where(year_val, medal_val)
    
    
    if not include_unknown:
        where_sql = add_condition(where_sql, "COALESCE(bc.country, 'Unknown') <> 'Unknown'")
        
    q = f"""
    WITH brewery_country AS (
        SELECT
            brewery_name_norm,
            MAX(country) AS country
        FROM breweries
        WHERE brewery_name_norm IS NOT NULL
        GROUP BY brewery_name_norm
    )
    SELECT
        COALESCE(bc.country, 'Unknown') AS country,
        COUNT(*) AS medals_total
    FROM awards a
    LEFT JOIN brewery_country bc
        ON a.brewery_name_norm = bc.brewery_name_norm
    {where_sql}
    GROUP BY COALESCE(bc.country, 'Unknown')
    ORDER BY medals_total DESC
    Limit ?;
        """
    params2 = params +[int(top_n)]
    with get_conn() as conn:
        df = pd.read_sql_query(q, conn, params=params2)
    return df

def country_match_rate(year_val, medal_val) -> dict:
    where_sql, params = build_where(year_val, medal_val)

    q=f"""
    WITH brewery_country AS (
        SELECT
            brewery_name_norm,
            MAX(country) AS country
        FROM breweries
        WHERE brewery_name_norm IS NOT NULL
        GROUP BY brewery_name_norm
    )
    SELECT
        COUNT(*) AS medals_total,
        SUM(
            CASE 
                WHEN bc.country IS NOT NULL AND TRIM(bc.country) <> '' THEN 1 
                ELSE 0 
            END
        ) AS matched_total
    FROM awards a
    LEFT JOIN brewery_country bc
        ON a.brewery_name_norm = bc.brewery_name_norm
        {where_sql};
        """
    
    with get_conn() as conn:
        row = pd.read_sql_query(q, conn, params=params).iloc[0].to_dict()
    
    medals_total = int(row.get("medals_total") or 0)
    matched_total = int(row.get("matched_total") or 0)
    rate = (matched_total / medals_total) if medals_total else 0.0

    return {"medals_total": medals_total, "matched_total": matched_total, "match_rate": rate}


def search_awards(brewery_query: str, year_val, medal_val, limit: int = 200) -> pd.DataFrame:
    where_sql, params = build_where(year_val, medal_val)

    extra = ""
    if brewery_query and brewery_query.strip():
        extra = " AND LOWER(a.brewery_name) LIKE ?"
        params.append(f"%{brewery_query.strip().lower()}%")

    q = f"""
    SELECT
        a.year,
        a.medal,
        a.category,
        a.brewery_name,
        a.beer_name
    FROM awards a
    {where_sql}
    {extra}
    ORDER BY a.year DESC, a.medal ASC, a.category ASC
    LIMIT ?;
    """
    with get_conn() as conn:
        df = pd.read_sql_query(q, conn, params=params + [int(limit)])
    return df

#starting dash 

app = Dash(__name__)
app.title = "Beer Awards Dashboard"
year_options, medal_options = load_filter_options()

app.layout = html.Div(
    style={"maxWidth": "1200px", "margin": "0 auto", "padding": "16px", "fontFamily": "system-ui"},
    children=[
        html.H1("Beer Awards Dashboard", style={"marginBottom": "2px"}),
        html.Div(
            "World Beer Cup medals + BJCP styles + OpenBreweryDB locations",
            style={"color": "#555", "marginBottom": "18px"},
        ),
        html.Div(
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "12px"},
            children=[
                html.Div(
                    style={"minWidth": "240px"},
                    children=[
                        html.Label("Year"),
                        dcc.Dropdown(id="year",options=year_options, value="ALL", clearable=False),
                    ],
                ),
                html.Div(
                    style={"minWidth": "240px"},
                    children=[
                        html.Label("Medal"),
                        dcc.Dropdown(id="medal", options=medal_options, value="ALL", clearable=False),
                    ],
                ),
                html.Div(
                    style={"minWidth": "240px"},
                    children=[
                        html.Label("Country Filter"),
                        dcc.Checklist(
                            id="show-unknown",
                            options=[{"label": "Include Unknown", "value": "yes"}],
                            value=[],
                            style={"marginTop": "6px"},
                        ),
                    ],
                ),
                html.Div(
                    style={"minWidth": "240px"},
                    children=[
                        html.Label("Top N Breweries"),
                        dcc.Slider(
                            id="topn",
                            min=5,
                            max=50,
                            step=5,
                            value=15,
                            marks={i:str(i) for i in range(5,51,5)},
                        ),
                    ],
                ),
            ],
        ),
        html.Div(
            id="kpi-row",
            style={"display": "flex", "gap": "12px", "flexWrap": "wrap", "marginBottom": "12px"},
        ),
        html.Div(
            style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "12px", "marginBottom": "12px"},
            children=[
                html.Div(
                    style={"border": "1px solid #ddd", "borderRadius": "10px", "padding": "12px"},
                    children=[
                        html.H3("Top Breweries", style={"marginTop":0}),
                        dcc.Graph(id="top-breweries"),
                    ],
                ),
                html.Div(
                    style={"border": "1px solid #ddd", "borderRadius": "10px", "padding": "12px"},
                    children=[
                        html.H3("Medals by Year", style={"marginTop":0}),
                        dcc.Graph(id="medals-by-year"),
                    ],
                ),
                html.Div(
                    style={"border": "1px solid #ddd", "borderRadius": "10px", "padding": "12px"},
                    children=[
                        html.H3("Medals by Country (OBDB match)", style={"marginTop": 0}),
                        html.Div(
                            "Note: Country derived from OpenBreweryDB matchin. Some international breweries may appear 'Unknown'.",
                            style={"fontSize": "12px", "color": "#666", "marginBottom": "6px"}
                        ),
                        dcc.Graph(id="medals-by-country"),
                    ],
                ),
                html.Div([
                    html.H4(" Country Match Rate"),
                    html.H2(id="kpi_match_rate", children="-"),
                    html.Div(id="kpi_match_detail", children="")
            ]),
            ],
        ),
        html.Div(
            style={"border": "1px solid #ddd", "borderRadius": "10px", "padding": "12px"},
            children=[
                html.H3("Search Awards", style={"marginTop": 0}),
                dcc.Input(
                    id="brewery-search",
                    type="text",
                    placeholder="Type part of a brewery name...",
                    style={"width": "100%", "padding": "10px", "marginBottom": "10px"},
                ),
                dash_table.DataTable(
                    id="results-table",
                    page_size=15,
                    style_table={"overflowX": "auto"},
                    style_cell={"textAlign": "left", "padding": "8px","fontFamily": "system-ui", "fontSize": "13px"},
                    style_header={"fontWeight": "bold"},
                ),
            ],
        ),
    ],
)

def kpi_card(title: str, value: str) -> html.Div:
    return html.Div(
        style={"border": "1px solid #ddd", "borderRadius": "10px", "padding": "12px", "minWidth": "220px"},
        children=[
            html.Div(title, style={"color": "#555", "fontSize": "13px"}),
            html.Div(value, style={"fontSize": "28px", "fontWeight": "700"}),
        ],
    )


@app.callback(
    Output("kpi-row", "children"),
    Output("top-breweries", "figure"),
    Output("medals-by-year", "figure"),
    Output("medals-by-country", "figure"),
    Output("kpi_match_rate", "children"),
    Output("kpi_match_detail", "children"),
    Input("year", "value"),
    Input("medal", "value"),
    Input("topn", "value"),
    Input("show-unknown", "value"),
)

def update_charts(year_val, medal_val, topn_val, show_unknown_vals):
    k = kpis(year_val,medal_val)
    topn = int(topn_val)
    kpi_children = [
        kpi_card("Total medals (filtered)", f"{k['medals_total']:,}"),
        kpi_card("Distinct breweries (filtered)", f"{k['breweries_total']:,}"),
        kpi_card("Distinct categories (filtered)", f"{k['categories_total']:,}"),
    ]

    top_df = top_breweries(year_val,medal_val,top_n=int(topn_val))

    if top_df.empty:
        fig_top = px.bar(title="No data (check brewery_name parsing)")
    else:
        fig_top = px.bar(
            top_df.sort_values(["medals_total", "golds"], ascending=True),
            x="medals_total",
            y="brewery_name",
            orientation="h",
            title="Top Breweries by Medals",
            template="plotly_white",
            color="medals_total",
            color_continuous_scale="Blues"
        )

    # Medals by year
    by_year = medals_by_year(medal_val)
    if by_year.empty:
        fig_year = px.line(title="No year data")
    else:
        fig_year = px.line(by_year, x="year", y="medals_total", markers=True, title="Medals over time", template="plotly_white")
    

    #medals by country
    include_unknown = "yes" in (show_unknown_vals or [])
    by_country = medals_by_country(year_val, medal_val, top_n=15, include_unknown=include_unknown)
    if by_country.empty:
        fig_country = px.bar(title="No Country Data (OBDB might be weak)")
    else:
        fig_country = px.bar(by_country, x="country", y="medals_total", title="Top Countries (matched)", template="plotly_white", color="medals_total", color_continuous_scale="Oranges")

    match_rate_pct = f"{k.get('match_rate', 0.0) * 100:.1f}%"
    match_detail = f"{k.get('medals_matched', 0):,} matched / {k.get('medals_total', 0):,} total medals"
    

    return kpi_children, fig_top, fig_year, fig_country, match_rate_pct, match_detail

@app.callback(
    Output("results-table", "data"),
    Output("results-table", "columns"),
    Input("brewery-search", "value"),
    Input("year", "value"),
    Input("medal", "value"),
)

def update_table(search_text, year_val, medal_val):
    df = search_awards(search_text or "", year_val, medal_val, limit=300)
    cols = [{"name": c, "id": c} for c in df.columns]
    return df.to_dict("records"), cols

if __name__ == "__main__":
    app.run(debug=True)