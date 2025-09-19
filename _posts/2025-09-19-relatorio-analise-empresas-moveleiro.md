---
layout: post
title: "Análise Comparativa - Empresas do Setor Moveleiro"
date: 2025-09-19 12:00:00 -0300
categories: [projetos, analise-financeira]
tags: [python, sql, analise]
---

# 📊 Análise Comparativa - Empresas do Setor Moveleiro

> **Relatório gerado automaticamente em:** 19/09/2025 10:39

## 🏢 Empresas Analisadas:
- **DXCO3.SA** - Dexco (antiga Duratex)
- **PTBL3.SA** - Portobello
- **WHR** - Whirlpool (NYSE)
- **ELET3.SA** - Eletrobras
- **MGEL4.SA** - Mangels Industrial
- **TEKA4.SA** - Tekno
- **PINE4.SA** - Eucatex

---

## 📈 1. Análise de Preços e Performance


### 📊 Carregando Dados de Preços

```python
# Configuração da conexão com o banco
db_config = {
    'host': 'localhost',
    'port': '5432',
    'database': 'yfinance_moveis',
    'user': 'postgres',
    'password': 'jc'
}

engine = create_engine(
    f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

# Query para obter preços mais recentes
query_latest_prices = """
SELECT
    sp.ticker,
    c.company_name,
    sp.date,
    sp.close_price,
    sp.volume,
    c.market_cap,
    c.sector,
    c.industry
FROM stock_prices sp
JOIN companies c ON sp.ticker = c.ticker
WHERE sp.date = (
    SELECT MAX(date)
    FROM stock_prices sp2
    WHERE sp2.ticker = sp.ticker
)
ORDER BY sp.close_price DESC;
"""

latest_prices = pd.read_sql(query_latest_prices, engine)
print("✅ Dados carregados com sucesso!")
display(latest_prices)
```

**Output:**

✅ Dados carregados com sucesso!

| Ticker | Empresa | Data | Preço (R$) | Volume | Market Cap |
|--------|---------|------|------------|--------|------------|
| WHR | Whirlpool Corporation | 2025-09-18 | 84.90 | 2,066,919 | 4,745,435,136 |
| ELET3.SA | Centrais Elétricas Brasileiras S.A. - Eletrobrás | 2025-09-18 | 49.00 | 16,355,300 | 110,974,910,464 |
| TEKA4.SA | Teka Tecelagem Kuehnrich S.A. | 2025-09-17 | 21.25 | 0 | 7,123,850 |
| PINE4.SA | Banco Pine S.A. | 2025-09-18 | 8.08 | 553,900 | 1,799,957,248 |
| DXCO3.SA | Dexco S.A. | 2025-09-18 | 6.02 | 2,282,400 | 4,878,637,056 |
| MGEL4.SA | Mangels Industrial S.A. | 2025-09-18 | 5.77 | 4,000 | 21,441,142 |
| PTBL3.SA | PBG S.A. | 2025-09-18 | 4.08 | 60,800 | 575,226,432 |


### 📈 Análise de Performance Multi-Período

```python
# Query para calcular performance em múltiplos períodos
query_performance = """
WITH price_performance AS (
    SELECT
        ticker,
        date,
        close_price,
        LAG(close_price, 30) OVER (PARTITION BY ticker ORDER BY date) as price_30d_ago,
        LAG(close_price, 90) OVER (PARTITION BY ticker ORDER BY date) as price_90d_ago,
        LAG(close_price, 252) OVER (PARTITION BY ticker ORDER BY date) as price_1y_ago
    FROM stock_prices
),
latest_performance AS (
    SELECT
        ticker,
        close_price as current_price,
        CASE
            WHEN price_30d_ago > 0
            THEN ROUND(((close_price - price_30d_ago) / price_30d_ago * 100), 2)
            ELSE NULL
        END as return_30d,
        CASE
            WHEN price_90d_ago > 0
            THEN ROUND(((close_price - price_90d_ago) / price_90d_ago * 100), 2)
            ELSE NULL
        END as return_90d,
        CASE
            WHEN price_1y_ago > 0
            THEN ROUND(((close_price - price_1y_ago) / price_1y_ago * 100), 2)
            ELSE NULL
        END as return_1y
    FROM price_performance
    WHERE date = (SELECT MAX(date) FROM stock_prices WHERE ticker = price_performance.ticker)
)
SELECT
    lp.ticker,
    c.company_name,
    lp.current_price,
    lp.return_30d,
    lp.return_90d,
    lp.return_1y,
    c.market_cap
FROM latest_performance lp
JOIN companies c ON lp.ticker = c.ticker
ORDER BY lp.return_30d DESC NULLS LAST;
"""

performance_data = pd.read_sql(query_performance, engine)
print("📊 Dados de performance carregados!")
display(performance_data)
```

**Output:**
📊 Dados de performance carregados!


```python
# Criando gráficos interativos de performance
from plotly.subplots import make_subplots
import plotly.graph_objects as go

fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=('Retorno 30 Dias (%)', 'Retorno 90 Dias (%)',
                   'Retorno 1 Ano (%)', 'Market Cap vs Preço Atual'),
    specs=[[{"type": "bar"}, {"type": "bar"}],
           [{"type": "bar"}, {"type": "scatter"}]]
)

# Retorno 30 dias
fig.add_trace(
    go.Bar(x=performance_data['ticker'], y=performance_data['return_30d'],
           name='30 dias', marker_color='lightblue'),
    row=1, col=1
)

# Retorno 90 dias
fig.add_trace(
    go.Bar(x=performance_data['ticker'], y=performance_data['return_90d'],
           name='90 dias', marker_color='orange'),
    row=1, col=2
)

# Retorno 1 ano
fig.add_trace(
    go.Bar(x=performance_data['ticker'], y=performance_data['return_1y'],
           name='1 ano', marker_color='green'),
    row=2, col=1
)

# Market Cap vs Preço
fig.add_trace(
    go.Scatter(x=performance_data['market_cap'], y=performance_data['current_price'],
               mode='markers+text', text=performance_data['ticker'],
               textposition='top center', name='Market Cap vs Preço',
               marker=dict(size=12, color='red')),
    row=2, col=2
)

fig.update_layout(height=800, showlegend=False,
                 title_text="📊 Análise de Performance das Empresas")
fig.show()
```

**Output:**


### Performance Multi-Período:

![Análise de Performance](/assets/img/posts/plotly_001_performance_analysis.png)

| Ticker | Empresa | Preço Atual | 30d (%) | 90d (%) | 1 ano (%) |
|--------|---------|-------------|---------|---------|-----------|
| MGEL4.SA | Mangels Industrial S.A. | 5.77 | 35.5% | -3.8% | -47.5% |
| PINE4.SA | Banco Pine S.A. | 8.08 | 34.4% | 57.4% | 85.5% |
| ELET3.SA | Centrais Elétricas Brasileiras S.A. - Eletrobrás | 49.00 | 19.3% | 18.2% | 28.6% |
| PTBL3.SA | PBG S.A. | 4.08 | 5.2% | -1.0% | -19.7% |
| DXCO3.SA | Dexco S.A. | 6.02 | 4.0% | 13.6% | -24.9% |
| WHR | Whirlpool Corporation | 84.90 | 3.4% | 9.4% | -10.1% |
| TEKA4.SA | Teka Tecelagem Kuehnrich S.A. | 21.25 | 0.0% | 0.0% | -26.7% |


---

## 💰 2. Análise Financeira - DRE



### Últimos Resultados Trimestrais:

![Análise Financeira](/assets/img/posts/plotly_002_financial_analysis.png)

| Ticker | Empresa | Receita (M) | Lucro Líq. (M) | Margem Bruta | Margem Líq. | EPS |
|--------|---------|-------------|----------------|--------------|-------------|-----|
| ELET3.SA | ELETROBRAS | 10199.0 | -1324.4 | 47.9% | -13.0% | nan |
| WHR | WHIRLPOOL | 3773.0 | 65.0 | 16.2% | 1.7% | 1.17 |
| DXCO3.SA | DEXCO | 2121.7 | 31.7 | 23.0% | 1.5% | 0.04 |
| PTBL3.SA | PORTOBELLO | 686.8 | -44.2 | 36.7% | -6.4% | -0.31 |
| PINE4.SA | PINERICA | 307.1 | 83.0 | nan% | 27.0% | nan |
| MGEL4.SA | MANGELS | 245.0 | 9.2 | 9.4% | 3.7% | 1.49 |
| TEKA4.SA | TEKNO | 90.9 | -66.8 | 25.8% | -73.4% | -132.70 |


### Heatmap de Margens:

![Heatmap de Margens](/assets/img/posts/matplotlib_003_margins_heatmap.png)



---

## 📋 Resumo Executivo

### 📊 Estatísticas do Setor:

- **Receita total combinada:** R$ 17423 milhões
- **Margem líquida média:** -8.4%
- **Margem bruta média:** 26.5%


### 📈 Performance de Preços:
- **Retorno médio 30 dias:** 14.5%
- **Retorno médio 1 ano:** -2.1%


### 📌 Principais Insights:
- Análise baseada em dados mais recentes disponíveis
- Dados coletados via Yahoo Finance e armazenados em PostgreSQL
- Total de 3 gráficos gerados automaticamente
- Recomenda-se análise complementar com dados fundamentalistas

---

## 🔧 Informações Técnicas

- **Data de geração:** 19/09/2025 10:39
- **Imagens geradas:** 3
- **Diretório de imagens:** `images/`

### Dependências:
```bash
pip install pandas matplotlib seaborn plotly sqlalchemy psycopg2-binary yfinance
```

### Execução:
```bash
python run_analysis_with_export.py
```
