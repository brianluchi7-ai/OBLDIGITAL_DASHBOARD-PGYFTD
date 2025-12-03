import re
import pandas as pd
import dash
from dash import html, dcc, Input, Output, dash_table
import plotly.express as px
from conexion_mysql import crear_conexion

# ======================================================
# === OBL DIGITAL DASHBOARD — DEP FTD (Dark Gold Theme)
# ======================================================

def cargar_datos():
    """Carga datos desde MySQL o CSV local."""
    try:
        conexion = crear_conexion()
        if conexion:
            print("✅ Leyendo desde Railway MySQL...")
            query = "SELECT * FROM FTD_MASTER_PGY_CLEAN"
            df = pd.read_sql(query, conexion)
            conexion.close()
            return df
    except Exception as e:
        print(f"⚠️ Error conectando a SQL, leyendo CSV local: {e}")

    print("📁 Leyendo desde CSV local...")
    return pd.read_csv("FTD_MASTER_PGY_preview.csv", dtype=str)


# === 1️⃣ Cargar datos ===
df = cargar_datos()
df.columns = [c.strip().lower() for c in df.columns]

# === 2️⃣ Normalizar fechas ===
def convertir_fecha(valor):
    try:
        if "/" in valor:
            return pd.to_datetime(valor, format="%d/%m/%Y", errors="coerce")
        elif "-" in valor:
            return pd.to_datetime(valor.split(" ")[0], errors="coerce")
    except Exception:
        return pd.NaT
    return pd.NaT

df["date"] = df["date"].astype(str).str.strip().apply(convertir_fecha)
df = df[df["date"].notna()]
df["date"] = pd.to_datetime(df["date"], utc=False).dt.tz_localize(None)

# === 3️⃣ Limpieza de USD ===
def limpiar_usd(valor):
    if pd.isna(valor): return 0.0
    s = str(valor).strip()
    if s == "": return 0.0

    s = re.sub(r"[^\d,.\-]", "", s)
    if "." in s and "," in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and not "." in s:
        partes = s.split(",")
        s = s.replace(",", ".") if len(partes[-1]) == 2 else s.replace(",", "")
    elif s.count(".") > 1:
        s = s.replace(".", "")

    try: return float(s)
    except: return 0.0

df["usd"] = df["usd"].apply(limpiar_usd)

# === 4️⃣ Limpieza de texto ===
for col in ["team", "agent", "country", "affiliate", "id"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.title()
        df[col].replace({"Nan": None, "None": None, "": None}, inplace=True)

fecha_min, fecha_max = df["date"].min(), df["date"].max()

# === 5️⃣ Formato K/M ===
def formato_km(valor):
    if valor >= 1_000_000:
        return f"{valor/1_000_000:.2f}M"
    elif valor >= 1_000:
        return f"{valor/1_000:.1f}K"
    else:
        return f"{valor:.0f}"

# === 6️⃣ Inicializar app con scripts externos ===
external_scripts = [
    "https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js",
    "https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js",
    "https://cdnjs.cloudflare.com/ajax/libs/pptxgenjs/3.10.0/pptxgen.bundle.js"
]

app = dash.Dash(__name__, external_scripts=external_scripts)
server = app.server
app.title = "OBL Digital — DEP FTD Dashboard"

# === 7️⃣ Layout ===
app.layout = html.Div(
    style={
        "backgroundColor": "#0d0d0d",
        "color": "#000000",
        "fontFamily": "Arial",
        "padding": "20px",
    },
    children=[
        html.H1("📊 DASHBOARD DEP FTD", style={
            "textAlign": "center",
            "color": "#D4AF37",
            "marginBottom": "30px",
            "fontWeight": "bold"
        }),

        html.Div(
            style={"display": "flex", "justifyContent": "space-between"},
            children=[
                # --- Panel de Filtros ---
                html.Div(
                    style={
                        "width": "25%",
                        "backgroundColor": "#1a1a1a",
                        "padding": "20px",
                        "borderRadius": "12px",
                        "boxShadow": "0 0 15px rgba(212,175,55,0.3)",
                        "textAlign": "center"
                    },
                    children=[
                        html.H4("Date", style={"color": "#D4AF37", "textAlign": "center"}),

                        dcc.DatePickerRange(
                            id="filtro-fecha",
                            start_date=fecha_min,
                            end_date=fecha_max,
                            display_format="YYYY-MM-DD",
                            style={"marginBottom": "20px", "textAlign": "center"},
                        ),

                        html.Div([
                            html.Label("Team", style={"color": "#D4AF37", "fontWeight": "bold"}),
                            dcc.Dropdown(sorted(df["team"].dropna().unique()), [], multi=True, id="filtro-team"),

                            html.Label("Agent", style={"color": "#D4AF37", "fontWeight": "bold"}),
                            dcc.Dropdown(sorted(df["agent"].dropna().unique()), [], multi=True, id="filtro-agent"),

                            html.Label("Country", style={"color": "#D4AF37", "fontWeight": "bold"}),
                            dcc.Dropdown(sorted(df["country"].dropna().unique()), [], multi=True, id="filtro-country"),

                            html.Label("Affiliate", style={"color": "#D4AF37", "fontWeight": "bold"}),
                            dcc.Dropdown(sorted(df["affiliate"].dropna().unique()), [], multi=True, id="filtro-affiliate"),

                            html.Label("ID", style={"color": "#D4AF37", "fontWeight": "bold"}),
                            dcc.Dropdown(sorted(df["id"].dropna().unique()), [], multi=True, id="filtro-id"),
                        ]),
                    ],
                ),

                # --- Panel principal ---
                html.Div(
                    style={"width": "72%"},
                    children=[
                        html.Div(
                            style={"display": "flex", "justifyContent": "space-around"},
                            children=[
                                html.Div(id="indicador-usuarios", style={"width": "30%"}),
                                html.Div(id="indicador-usd", style={"width": "30%"}),
                                html.Div(id="indicador-target", style={"width": "30%"}),
                            ],
                        ),
                        html.Br(),

                        html.Div(
                            style={"display": "flex", "flexWrap": "wrap", "gap": "20px"},
                            children=[
                                dcc.Graph(id="grafico-usd-country", style={"width": "48%", "height": "340px"}),
                                dcc.Graph(id="grafico-usd-affiliate", style={"width": "48%", "height": "340px"}),
                                dcc.Graph(id="grafico-usd-team", style={"width": "48%", "height": "340px"}),
                                dcc.Graph(id="grafico-usd-date", style={"width": "48%", "height": "340px"}),
                            ],
                        ),
                        html.Br(),

                        html.H4("📋 Detalle de transacciones", style={"color": "#D4AF37"}),
                        dash_table.DataTable(
                            id="tabla-detalle",
                            columns=[{"name": i.upper(), "id": i} for i in df.columns],
                            style_table={"overflowX": "auto", "backgroundColor": "#0d0d0d"},
                            page_size=10,
                            style_cell={"textAlign": "center", "color": "#f2f2f2", "backgroundColor": "#1a1a1a"},
                            style_header={"backgroundColor": "#D4AF37", "color": "#000", "fontWeight": "bold"},
                        ),
                    ],
                ),
            ],
        ),
    ],
)

# === 8️⃣ Callback ===
@app.callback(
    [
        Output("indicador-usuarios", "children"),
        Output("indicador-usd", "children"),
        Output("indicador-target", "children"),
        Output("grafico-usd-country", "figure"),
        Output("grafico-usd-affiliate", "figure"),
        Output("grafico-usd-team", "figure"),
        Output("grafico-usd-date", "figure"),
        Output("tabla-detalle", "data"),
    ],
    [
        Input("filtro-fecha", "start_date"),
        Input("filtro-fecha", "end_date"),
        Input("filtro-team", "value"),
        Input("filtro-agent", "value"),
        Input("filtro-country", "value"),
        Input("filtro-affiliate", "value"),
        Input("filtro-id", "value"),
    ],
)
def actualizar_dashboard(start, end, team, agent, country, affiliate, id_user):
    df_filtrado = df.copy()

    if start and end:
        start, end = pd.to_datetime(start), pd.to_datetime(end)
        df_filtrado = df_filtrado[(df_filtrado["date"] >= start) & (df_filtrado["date"] <= end)]

    if team: df_filtrado = df_filtrado[df_filtrado["team"].isin(team)]
    if agent: df_filtrado = df_filtrado[df_filtrado["agent"].isin(agent)]
    if country: df_filtrado = df_filtrado[df_filtrado["country"].isin(country)]
    if affiliate: df_filtrado = df_filtrado[df_filtrado["affiliate"].isin(affiliate)]
    if id_user: df_filtrado = df_filtrado[df_filtrado["id"].isin(id_user)]

    total_usd = df_filtrado["usd"].sum()
    total_users = df_filtrado["id"].nunique()
    target = total_usd * 1.1

    card_style = {
        "backgroundColor": "#1a1a1a",
        "borderRadius": "10px",
        "padding": "20px",
        "width": "80%",
        "textAlign": "center",
        "boxShadow": "0 0 10px rgba(212,175,55,0.3)",
    }

    indicador_usuarios = html.Div([
        html.H4("MOUNT USERS", style={"color": "#D4AF37", "fontWeight": "bold"}),
        html.H2(f"{total_users:,}", style={"color": "#FFFFFF", "fontSize": "36px"})
    ], style=card_style)

    indicador_usd = html.Div([
        html.H4("TOTAL USD", style={"color": "#D4AF37", "fontWeight": "bold"}),
        html.H2(formato_km(total_usd), style={"color": "#FFFFFF", "fontSize": "36px"})
    ], style=card_style)

    indicador_target = html.Div([
        html.H4("TARGET", style={"color": "#D4AF37", "fontWeight": "bold"}),
        html.H2(formato_km(target), style={"color": "#FFFFFF", "fontSize": "36px"})
    ], style=card_style)

    fig_country = px.pie(df_filtrado, names="country", values="usd", title="USD by Country", color_discrete_sequence=px.colors.sequential.YlOrBr)
    fig_affiliate = px.pie(df_filtrado, names="affiliate", values="usd", title="USD by Affiliate", color_discrete_sequence=px.colors.sequential.YlOrBr)
    fig_team = px.bar(df_filtrado.groupby("team", as_index=False)["usd"].sum(), x="team", y="usd", title="USD by Team", color="usd", color_continuous_scale="YlOrBr")
    fig_usd_date = px.line(df_filtrado.sort_values("date"), x="date", y="usd", title="USD by Date", markers=True, color_discrete_sequence=["#D4AF37"])

    for fig in [fig_country, fig_affiliate, fig_team, fig_usd_date]:
        fig.update_layout(paper_bgcolor="#0d0d0d", plot_bgcolor="#0d0d0d", font_color="#f2f2f2", title_font_color="#D4AF37")

    return indicador_usuarios, indicador_usd, indicador_target, fig_country, fig_affiliate, fig_team, fig_usd_date, df_filtrado.to_dict("records")


# === 9️⃣ Captura PDF/PPT desde iframe ===
app.index_string = '''
<!DOCTYPE html>
<html>
<head>
  {%metas%}
  <title>OBL Digital — Dashboard FTD</title>
  {%favicon%}
  {%css%}
  <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
</head>
<body>
  {%app_entry%}
  <footer>
    {%config%}
    {%scripts%}
    {%renderer%}
  </footer>

  <script>
    window.addEventListener("message", async (event) => {
      if (!event.data || event.data.action !== "capture_dashboard") return;

      try {
        const canvas = await html2canvas(document.body, { useCORS: true, scale: 2, backgroundColor: "#0d0d0d" });
        const imgData = canvas.toDataURL("image/png");

        window.parent.postMessage({
          action: "capture_image",
          img: imgData,
          filetype: event.data.type
        }, "*");
      } catch (err) {
        console.error("Error al capturar dashboard:", err);
        window.parent.postMessage({ action: "capture_done" }, "*");
      }
    });
  </script>
</body>
</html>
'''

if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True)




