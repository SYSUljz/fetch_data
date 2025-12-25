
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def visualize_labels(df, output_file="label_visualization.html"):
    # 1. 定义需要绘制的标签列
    #label_cols = ['label_5', 'label_10', 'label_30', 'label_50', 'label']
    label_cols = ['label']
    # 2. 创建子图布局
    # 第一行画价格，后面五行画对应的 label
    # row_heights 设置比例：价格占 40%，每个 label 各占 12%
    fig = make_subplots(
        rows=6, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.02,
        subplot_titles=("Mid Price", *label_cols),
        row_heights=[0.4, 0.12, 0.12, 0.12, 0.12, 0.12]
    )

    # 3. 绘制 Mid Price (主图)
    fig.add_trace(
        go.Scatter(x=df['exchange_time'], y=df['mid_price'], name='Mid Price', line=dict(color='black', width=1)),
        row=1, col=1
    )

    # 4. 循环绘制各个 Label
    # 为了方便观察，我们将 label 映射为颜色：-1 (红), 0 (灰), 1 (绿)
    color_map = {-1: 'red', 0: 'lightgrey', 1: 'green'}
    
    for i, col in enumerate(label_cols):
        # 这里的 i+2 是因为第一行被价格占了
        curr_row = i + 2
        
        # 绘制散点图，通过颜色区分标签
        # 使用 Scattergl (WebGL) 在大数据量下性能更好
        for val, color in color_map.items():
            mask = df[col] == val
            fig.add_trace(
                go.Scattergl(
                    x=df.loc[mask, 'exchange_time'],
                    y=df.loc[mask, col],
                    mode='markers',
                    marker=dict(size=5, color=color),
                    name=f"{col}_{val}",
                    showlegend=False # 隐藏过多的图例
                ),
                row=curr_row, col=1
            )
        
        # 设置 Y 轴刻度，只显示 -1, 0, 1
        fig.update_yaxes(tickvals=[-1, 0, 1], row=curr_row, col=1)

    # 5. 更新布局配置
    fig.update_layout(
        title_text=f"Dataset Label Analysis (Session: {df['session_id'].iloc[0] if 'session_id' in df.columns else 'All'})",
        height=1200, # 增加高度以便看得更清楚
        showlegend=True,
        template="plotly_white",
        xaxis6_title="Exchange Time", # 最下方的 X 轴标题
    )

    # 6. 保存为 HTML
    fig.write_html(output_file)
    print(f"可视化文件已生成: {output_file}")
