import plotly.express as px
import plotly.graph_objects as go

def plot_category_count(df, column_name, title="Category Count", xaxis_title="Category", yaxis_title="Count"):
    category_counts = df[column_name].value_counts().reset_index()
    category_counts.columns = [column_name, 'Count']
    
    fig = px.bar(category_counts, x=column_name, y='Count', title=title)
    fig.update_layout(
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title
    )
    return fig

def plot_horizontal_bar(df, x_column, y_column, title="Horizontal Bar Chart", xaxis_title="Value", yaxis_title="Category", orientation='h'):
    df_sorted = df.sort_values(by=x_column, ascending=True)
    
    fig = px.bar(df_sorted, x=x_column, y=y_column, title=title, orientation=orientation)
    fig.update_layout(
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title
    )
    return fig

def plot_dividend_chart(df, date_column, value_column, yield_column, title="Dividendos e Dividend Yield"):
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df[date_column],
        y=df[value_column],
        name='Dividendos (R$)',
        marker_color='rgba(131, 175, 210, 0.6)',
        yaxis='y',
        text=df[value_column],
        textposition='outside',
    ))

    fig.add_trace(go.Scatter(
        x=df[date_column],
        y=df[yield_column],
        name='Dividend Yield (%)',
        mode='lines+markers',
        marker=dict(color='rgba(120, 200, 120, 0.6)'),
        line=dict(color='rgba(120, 200, 120, 0.6)'),
        yaxis='y2'
    ))

    fig.update_layout(
        title=title,
        xaxis_title="Período",
        yaxis_title="Dividendos (R$)",
        yaxis=dict(
            title="Dividendos (R$)",
            side='left'
        ),
        yaxis2=dict(
            title="Dividend Yield (%)",
            overlaying='y',
            side='right'
        ),
        legend=dict(x=0.1, y=1.1, orientation="h"),
        barmode='group'
    )
    return fig

