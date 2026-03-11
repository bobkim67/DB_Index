"""
pension_index_dash.py — DB형 퇴직연금 부채 인덱스 대시보드
pre-computed CSV 로드 (val.tsv 재로드 없음)
"""
import dash
from dash import dcc, html, Input, Output, callback
import dash_ag_grid as dag
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path(__file__).parent

# ═══════════════════════════════════════════════════════════
# 데이터 로드
# ═══════════════════════════════════════════════════════════
df_data = pd.read_csv(BASE / 'pension_index_data.csv')
df_result = pd.read_csv(BASE / 'pension_index_result.csv')

YEARS = sorted(df_result['year_end'].unique())
TIER_LABELS = ['1_대형', '2_중형', '3_중소형', '4_소형']
TIER_COLORS = {'1_대형': '#1f77b4', '2_중형': '#ff7f0e', '3_중소형': '#2ca02c', '4_소형': '#d62728'}
INDEX_COLORS = {'Tier균등': '#1f77b4', 'DBO가중': '#ff7f0e', '동일가중': '#2ca02c'}

# ── 헬퍼 ──
def _val(col, year=None, default=np.nan):
    """df_result에서 값 추출"""
    if year is not None:
        row = df_result[df_result['year_end'] == year]
    else:
        row = df_result.iloc[[-1]]  # 최신
    if row.empty or col not in row.columns:
        return default
    v = row[col].iloc[0]
    return v if pd.notna(v) else default


def _fmt_조(v):
    if pd.isna(v): return '-'
    return f"{v / 1e12:.1f}조원"


def _fmt_pct(v, digits=1):
    if pd.isna(v): return '-'
    return f"{v * 100:.{digits}f}%"


def _fmt_idx(v):
    if pd.isna(v): return '-'
    return f"{v:.2f}"


def kpi_card(title, value, sub=''):
    return html.Div([
        html.P(title, style={'margin': '0', 'fontSize': '0.85rem', 'color': '#666'}),
        html.H3(value, style={'margin': '4px 0', 'color': '#1f77b4', 'fontSize': '1.4rem'}),
        html.P(sub, style={'margin': '0', 'fontSize': '0.8rem', 'color': '#999'}) if sub else None,
    ], style={
        'padding': '16px 20px', 'borderRadius': '8px', 'backgroundColor': 'white',
        'boxShadow': '0 1px 3px rgba(0,0,0,0.12)', 'textAlign': 'center',
        'flex': '1', 'minWidth': '160px',
    })


CARD_ROW = {'display': 'flex', 'gap': '12px', 'marginBottom': '20px', 'flexWrap': 'wrap'}
CHART_BOX = {'backgroundColor': 'white', 'borderRadius': '8px',
             'boxShadow': '0 1px 3px rgba(0,0,0,0.12)', 'padding': '16px', 'marginBottom': '20px'}


# ═══════════════════════════════════════════════════════════
# Tab 1: 인덱스 추이
# ═══════════════════════════════════════════════════════════
def tab1_layout():
    latest = max(YEARS)
    idx_val = _val('index_tiered', latest)
    n_cik = int(_val('n_cik', latest, 0))
    fr = _val('funding_ratio', latest)
    total_dbo = _val('total_dbo', latest)

    cagr = np.nan
    if len(YEARS) >= 3 and idx_val > 0:
        cagr = (idx_val / 100) ** (1 / (latest - min(YEARS))) - 1

    # 인덱스 라인 차트
    fig = go.Figure()
    for col, name, dash_style in [
        ('index_tiered', 'Tier균등', 'solid'),
        ('index_dbo_weighted', 'DBO가중', 'dash'),
        ('index_equal_weighted', '동일가중', 'dot'),
    ]:
        vals = [_val(col, y) for y in YEARS]
        fig.add_trace(go.Scatter(
            x=[str(y) for y in YEARS], y=vals, name=name, mode='lines+markers+text',
            text=[_fmt_idx(v) for v in vals], textposition='top center',
            line=dict(width=3 if dash_style == 'solid' else 2, dash=dash_style,
                      color=INDEX_COLORS[name]),
        ))
    fig.add_hline(y=100, line_dash='dash', line_color='#ccc',
                  annotation_text='기준선 (2022=100)', annotation_position='top left')
    fig.update_layout(
        title='DB형 퇴직연금 부채 인덱스 (2022=100)',
        xaxis_title='연도말', yaxis_title='인덱스',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
        margin=dict(t=80, b=40), height=420,
    )

    # YoY 테이블
    yoy_data = []
    for y in YEARS:
        row = {'연도': f'{y}末'}
        for col, name in [('index_tiered', 'Tier균등'), ('index_dbo_weighted', 'DBO가중'),
                          ('index_equal_weighted', '동일가중')]:
            row[name] = _fmt_idx(_val(col, y))
            yoy_col = f'{col}_yoy'
            row[f'{name} YoY'] = _fmt_pct(_val(yoy_col, y))
        yoy_data.append(row)

    return html.Div([
        html.Div([
            kpi_card('현재 인덱스 (Tier균등)', _fmt_idx(idx_val), f'{latest}末'),
            kpi_card('CAGR', _fmt_pct(cagr), f'{min(YEARS)}-{latest}'),
            kpi_card('총 DBO', _fmt_조(total_dbo), f'Balanced {n_cik} CIK'),
            kpi_card('적립비율', _fmt_pct(fr), 'PA / DBO'),
        ], style=CARD_ROW),

        html.Div([dcc.Graph(figure=fig)], style=CHART_BOX),

        html.Div([
            html.H4("연도별 인덱스 및 YoY 변동", style={'marginBottom': '8px'}),
            dag.AgGrid(
                rowData=yoy_data,
                columnDefs=[{'field': c, 'width': 120} for c in yoy_data[0].keys()],
                defaultColDef={'sortable': False, 'resizable': True},
                style={'height': '180px'},
                dashGridOptions={'domLayout': 'autoHeight'},
            ),
        ], style=CHART_BOX),
    ])


# ═══════════════════════════════════════════════════════════
# Tab 2: Tier 분석
# ═══════════════════════════════════════════════════════════
def tab2_layout():
    # Tier별 성장률 bar chart
    bar_data = []
    for y in YEARS:
        for t in TIER_LABELS:
            g = _val(f'growth_{t}', y)
            n = _val(f'n_{t}', y, 0)
            bar_data.append({'연도': str(y), 'Tier': t, '평균성장률': g if pd.notna(g) else np.nan,
                             'CIK수': int(n) if pd.notna(n) else 0})
    df_bar = pd.DataFrame(bar_data).dropna(subset=['평균성장률'])

    fig_bar = px.bar(
        df_bar, x='연도', y='평균성장률', color='Tier', barmode='group',
        text_auto='.3f', color_discrete_map=TIER_COLORS,
        title='Tier별 평균 DBO 성장률 (vs 2022=1.0)',
    )
    fig_bar.add_hline(y=1.0, line_dash='dash', line_color='#ccc')
    fig_bar.update_layout(margin=dict(t=60, b=40), height=380)

    # Tier 구성 테이블
    tier_rows = []
    for t in TIER_LABELS:
        t_data = df_data[df_data['tier'] == t]
        row = {'Tier': t, 'CIK수': len(t_data)}
        for y in ['2022', '2023', '2024']:
            col = f'DBO_{y}'
            if col in t_data.columns:
                total = t_data[col].sum()
                row[f'DBO합계_{y}'] = f"{total / 1e12:.2f}조" if pd.notna(total) else '-'
                mean = t_data[col].mean()
                row[f'DBO평균_{y}'] = f"{mean / 1e9:.1f}십억" if pd.notna(mean) else '-'
        tier_rows.append(row)

    tier_cols = [{'field': k, 'width': 130} for k in tier_rows[0].keys()]

    # DBO 분포 box plot (2024末)
    df_box = df_data[df_data['tier'].notna()].copy()
    df_box['DBO_2024_십억'] = df_box.get('DBO_2024', pd.Series(dtype=float)) / 1e9
    fig_box = px.box(
        df_box.dropna(subset=['DBO_2024_십억']),
        x='tier', y='DBO_2024_십억', color='tier',
        color_discrete_map=TIER_COLORS,
        title='Tier별 DBO 분포 (2024末, 십억원)',
        labels={'tier': 'Tier', 'DBO_2024_십억': 'DBO (십억원)'},
    )
    fig_box.update_layout(showlegend=False, margin=dict(t=60, b=40), height=380)

    return html.Div([
        html.Div([dcc.Graph(figure=fig_bar)], style=CHART_BOX),

        html.Div([
            html.H4("Tier 구성 요약", style={'marginBottom': '8px'}),
            dag.AgGrid(
                rowData=tier_rows, columnDefs=tier_cols,
                defaultColDef={'sortable': True, 'resizable': True},
                style={'height': '220px'},
                dashGridOptions={'domLayout': 'autoHeight'},
            ),
        ], style=CHART_BOX),

        html.Div([dcc.Graph(figure=fig_box)], style=CHART_BOX),
    ])


# ═══════════════════════════════════════════════════════════
# Tab 3: CIK 상세
# ═══════════════════════════════════════════════════════════
def _format_grid_data():
    """AgGrid용 데이터 준비"""
    df = df_data.copy()
    # 금액을 억원으로 변환 (표시용)
    for c in df.columns:
        if c.startswith('DBO_') or c.startswith('PA_'):
            df[c + '_억'] = (df[c] / 1e8).round(1)
    # growth를 %로
    for c in df.columns:
        if c.startswith('growth_'):
            yr = c.split('_')[1]
            df[f'성장률_{yr}'] = (df[c] * 100).round(2)
    return df


def tab3_layout():
    df = _format_grid_data()

    col_defs = [
        {'field': 'CIK', 'width': 110, 'pinned': 'left', 'checkboxSelection': True},
        {'field': 'tier', 'headerName': 'Tier', 'width': 100},
        {'field': 'outlier_flag', 'headerName': '이상치', 'width': 120},
    ]
    for y in ['2022', '2023', '2024']:
        col_defs.append({'field': f'DBO_{y}_억', 'headerName': f'DBO {y} (억)', 'width': 130,
                         'type': 'numericColumn', 'valueFormatter': {'function': 'd3.format(",.1f")(params.value)'}})
        col_defs.append({'field': f'PA_{y}_억', 'headerName': f'PA {y} (억)', 'width': 120,
                         'type': 'numericColumn', 'valueFormatter': {'function': 'd3.format(",.1f")(params.value)'}})
    for y in ['2023', '2024']:
        col_defs.append({'field': f'성장률_{y}', 'headerName': f'성장률 {y}(%)', 'width': 120,
                         'type': 'numericColumn'})

    return html.Div([
        html.Div([
            html.H4("CIK별 상세 데이터", style={'marginBottom': '8px'}),
            html.P(f"총 {len(df)}개 CIK (필터/정렬/Excel 내보내기 가능)",
                   style={'color': '#666', 'fontSize': '0.85rem'}),
            dag.AgGrid(
                id='cik-grid',
                rowData=df.to_dict('records'),
                columnDefs=col_defs,
                defaultColDef={'sortable': True, 'filter': True, 'resizable': True},
                dashGridOptions={
                    'rowSelection': {'mode': 'single'},
                    'animateRows': True,
                    'pagination': True,
                    'paginationPageSize': 50,
                },
                csvExportParams={'fileName': 'pension_index_cik.csv'},
                style={'height': '500px'},
            ),
        ], style=CHART_BOX),

        html.Div([
            html.H4("선택 CIK DBO/PA 추이", style={'marginBottom': '8px'}),
            dcc.Graph(id='cik-detail-chart', style={'height': '350px'}),
        ], style=CHART_BOX),
    ])


# ═══════════════════════════════════════════════════════════
# Tab 4: 부가 지표
# ═══════════════════════════════════════════════════════════
def tab4_layout():
    charts = []

    # 1. 적립비율 추이
    fr_vals = [_val('funding_ratio', y) for y in YEARS]
    fig_fr = go.Figure()
    fig_fr.add_trace(go.Scatter(
        x=[str(y) for y in YEARS], y=[v * 100 if pd.notna(v) else None for v in fr_vals],
        mode='lines+markers+text', name='적립비율',
        text=[_fmt_pct(v) for v in fr_vals], textposition='top center',
        line=dict(width=3, color='#1f77b4'),
    ))
    fig_fr.update_layout(title='총 적립비율 추이 (PA / DBO)', yaxis_title='%',
                         margin=dict(t=60, b=40), height=350)
    charts.append(html.Div([dcc.Graph(figure=fig_fr)], style=CHART_BOX))

    # 2. 할인율 / 듀레이션 / 임금상승률
    rate_vars = [
        ('DiscountRate', '할인율'),
        ('SalaryGrowth', '임금상승률'),
        ('Duration', '듀레이션'),
    ]
    has_rates = any(f'{v[0]}_dbo_wgt' in df_result.columns for v in rate_vars)
    if has_rates:
        fig_rates = go.Figure()
        for var, name in rate_vars:
            col_wgt = f'{var}_dbo_wgt'
            col_n = f'{var}_n'
            if col_wgt not in df_result.columns:
                continue
            vals = [_val(col_wgt, y) for y in YEARS]
            ns = [_val(col_n, y, 0) for y in YEARS]
            fig_rates.add_trace(go.Bar(
                x=[str(y) for y in YEARS], y=vals, name=f'{name} (n={int(max(ns, default=0))})',
                text=[f'{v:.4f}' if pd.notna(v) else '' for v in vals],
                textposition='outside',
            ))
        fig_rates.update_layout(
            title='DBO가중 평균 보험수리적 가정',
            barmode='group', margin=dict(t=60, b=40), height=380,
            yaxis_title='값 (PURE 단위)',
        )
        charts.append(html.Div([dcc.Graph(figure=fig_rates)], style=CHART_BOX))

    # 3. DBO 변동 요인 분해 (2023→2024)
    latest = max(YEARS)
    if latest >= 2024:
        svc = _val('ServiceCost_total', 2024)
        intc = _val('InterestCost_total', 2024)
        bp = _val('BenefitPayment_total', 2024)
        agl = _val('ActuarialGL_total', 2024)

        has_decomp = any(pd.notna(v) for v in [svc, intc, bp, agl])
        if has_decomp:
            dbo_2023 = _val('total_dbo', 2023)
            dbo_2024 = _val('total_dbo', 2024)

            components = []
            vals = []
            measures = []

            components.append('DBO 2023末')
            vals.append(dbo_2023)
            measures.append('absolute')

            for label, v, sign in [
                ('근무원가', svc, 1), ('이자비용', intc, 1),
                ('급여지급', bp, -1), ('보험수리적손익', agl, 1),
            ]:
                if pd.notna(v):
                    components.append(label)
                    vals.append(v * sign)
                    measures.append('relative')

            # 잔차
            explained = sum(v for v, m in zip(vals[1:], measures[1:]))
            residual = dbo_2024 - dbo_2023 - explained
            if abs(residual) > 1e6:
                components.append('기타/잔차')
                vals.append(residual)
                measures.append('relative')

            components.append('DBO 2024末')
            vals.append(dbo_2024)
            measures.append('total')

            fig_wf = go.Figure(go.Waterfall(
                orientation='v',
                x=components,
                y=[v / 1e12 for v in vals],  # 조원 단위
                measure=measures,
                text=[f'{v / 1e12:.2f}조' for v in vals],
                textposition='outside',
                connector=dict(line=dict(color='#ccc', dash='dot')),
            ))
            fig_wf.update_layout(
                title='DBO 변동 요인 분해 (2023末→2024末, 조원)',
                yaxis_title='조원', margin=dict(t=60, b=40), height=420,
            )
            charts.append(html.Div([dcc.Graph(figure=fig_wf)], style=CHART_BOX))

    if not charts:
        charts.append(html.Div([html.P("부가 지표 데이터 없음")], style=CHART_BOX))

    return html.Div(charts)


# ═══════════════════════════════════════════════════════════
# App Layout & Callbacks
# ═══════════════════════════════════════════════════════════
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "DB형 퇴직연금 부채 인덱스"

app.layout = html.Div([
    html.Div([
        html.H2("DB형 퇴직연금 부채 인덱스", style={'margin': '0', 'color': 'white'}),
        html.P("DART XBRL 기반 | 2022=100 | 총액분리 CIK 대상",
               style={'margin': '4px 0 0', 'color': '#b0c4de', 'fontSize': '0.9rem'}),
    ], style={
        'backgroundColor': '#1a2744', 'padding': '20px 32px', 'marginBottom': '20px',
    }),

    html.Div([
        dcc.Tabs(id='main-tabs', value='tab1', children=[
            dcc.Tab(label='인덱스 추이', value='tab1'),
            dcc.Tab(label='Tier 분석', value='tab2'),
            dcc.Tab(label='CIK 상세', value='tab3'),
            dcc.Tab(label='부가 지표', value='tab4'),
        ], style={'marginBottom': '16px'}),
        html.Div(id='tab-content'),
    ], style={'padding': '0 32px 32px'}),
], style={'backgroundColor': '#f0f2f5', 'minHeight': '100vh', 'fontFamily': 'sans-serif'})


@callback(Output('tab-content', 'children'), Input('main-tabs', 'value'))
def render_tab(tab):
    if tab == 'tab1':
        return tab1_layout()
    if tab == 'tab2':
        return tab2_layout()
    if tab == 'tab3':
        return tab3_layout()
    if tab == 'tab4':
        return tab4_layout()
    return html.Div()


@callback(
    Output('cik-detail-chart', 'figure'),
    Input('cik-grid', 'selectedRows'),
    prevent_initial_call=True,
)
def update_cik_chart(selected):
    if not selected:
        return go.Figure().update_layout(
            annotations=[dict(text='CIK를 선택하세요', showarrow=False, font=dict(size=16))]
        )

    cik = selected[0].get('CIK', '')
    row = df_data[df_data['CIK'] == cik]
    if row.empty:
        return go.Figure()

    row = row.iloc[0]
    fig = go.Figure()

    for var, color, name in [('DBO', '#1f77b4', 'DBO'), ('PA', '#ff7f0e', 'PA')]:
        ys = []
        for y in ['2022', '2023', '2024']:
            col = f'{var}_{y}'
            ys.append(row.get(col, np.nan))
        fig.add_trace(go.Scatter(
            x=['2022', '2023', '2024'], y=ys, name=name,
            mode='lines+markers+text',
            text=[f'{v / 1e8:.0f}억' if pd.notna(v) else '' for v in ys],
            textposition='top center',
            line=dict(width=2, color=color),
        ))

    tier = row.get('tier', '-')
    fig.update_layout(
        title=f'CIK {cik} | Tier: {tier}',
        xaxis_title='연도말', yaxis_title='금액 (원)',
        margin=dict(t=60, b=40), height=340,
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
    )
    return fig


if __name__ == '__main__':
    app.run(debug=True, port=8050)
