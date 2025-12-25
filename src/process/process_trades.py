import pandas as pd
import glob
import os

def merge_parquet(folder, output_file):
    files = glob.glob(os.path.join(folder, "*.parquet"))
    
    if not files:
        print("未找到Parquet文件。")
        return

    dfs = []
    # 如果输出文件也在这个文件夹里，需要避免把它自己也读进去（取决于你的文件名规则）
    # 建议先过滤掉 output_file
    files = [f for f in files if os.path.abspath(f) != os.path.abspath(output_file)]

    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"跳过损坏文件: {f}, 错误: {e}")
    
    if not dfs:
        print("没有有效的数据被读取。")
        return

    # 合并数据
    df_merged = pd.concat(dfs, axis=0, ignore_index=True)
    df_merged = df_merged.dropna(axis=1, how='all')
    
    # ---------------------------------------------------------
    # 关键修改：步骤 1 - 先尝试保存文件
    # ---------------------------------------------------------
    try:
        folder = os.path.dirname(output_file)

        # 如果目录不存在就创建
        os.makedirs(folder, exist_ok=True)

        # 保存 parquet
        df_merged.to_parquet(output_file)
        print(f"合并成功，文件已保存至: {output_file}")
        
        # -----------------------------------------------------
        # 关键修改：步骤 2 - 保存成功后，不删除源文件
        # -----------------------------------------------------
                
        print("全部完成。")
        
    except Exception as e:
        # 如果保存失败，打印错误，并且绝对不要删除源文件
        print(f"!!! 保存失败 !!! 源文件未被删除。错误信息: {e}")
        
        
        
import pandas as pd
import numpy as np

def sample_lob_by_dollar_volume(lob_df, trades_df, threshold):
    """
    基于成交额阈值对LOB数据进行采样。
    
    参数:
    lob_df (pd.DataFrame): LOB数据，Index为datetime
    trades_df (pd.DataFrame): 成交数据，Index为datetime，必须包含 'values' 列
    threshold (float): 美元成交额阈值 (Dollar Bar Threshold)
    
    返回:
    pd.DataFrame: 采样后的LOB数据，包含 'interval_volume' 和 'skipped_snapshots'
    """
    # 1. 数据对齐与预处理
    # 确保数据按时间排序
    lob_df = lob_df.sort_index()
    trades_df = trades_df.sort_index()
    
    # 提取numpy数组以加速计算
    lob_times = lob_df.index.values
    trade_times = trades_df.index.values
    trade_values = trades_df['values'].values
    
    # 2. 计算成交额的累积和 (Cumulative Sum)
    # 在头部插入0，方便计算差值
    trade_cum_vol = np.concatenate(([0], np.cumsum(trade_values)))
    
    # 3. 将 LOB 的时间戳映射到 Trade 的累积成交额索引上
    # searchsorted(side='right') 会找到 <= lob_time 的所有 trade 的最后一个索引
    # 这意味着我们获取的是截止到该 LOB 切片时刻的总累计成交额
    idx_map = np.searchsorted(trade_times, lob_times, side='right')
    
    # 获取每个 LOB 切片时刻对应的全局累计成交额
    lob_snapshot_cum_vols = trade_cum_vol[idx_map]
    
    # 4. 核心逻辑：遍历并判定阈值
    # 这里虽然用了循环，但是是对一维数组的简单操作，比操作DataFrame快几个数量级
    
    keep_indices = []       # 保留的 LOB 行号索引
    interval_volumes = []   # 该区间的实际成交额
    skipped_counts = []     # 跳过的切片数量
    
    last_accepted_vol = lob_snapshot_cum_vols[0] # 上一次被保留切片时的累计成交额
    current_skipped = 0     # 当前累积跳过的切片数
    
    # 从第1行开始遍历 (第0行通常作为基准或根据需求决定，这里我们计算从第0行之后的变化)
    for i in range(1, len(lob_times)):
        current_vol = lob_snapshot_cum_vols[i]
        
        # 计算当前切片与上一个"保留切片"之间的成交额差值
        vol_diff = current_vol - last_accepted_vol
        
        if vol_diff >= threshold:
            # 触发阈值，保留该切片
            keep_indices.append(i)
            interval_volumes.append(vol_diff)
            skipped_counts.append(current_skipped)
            
            # 更新状态
            last_accepted_vol = current_vol
            current_skipped = 0
        else:
            # 未达到阈值，跳过
            current_skipped += 1
            
    # 5. 构建输出 DataFrame
    if not keep_indices:
        return pd.DataFrame() # 如果没有切片满足条件
        
    sampled_df = lob_df.iloc[keep_indices].copy()
    sampled_df['interval_volume'] = interval_volumes
    sampled_df['skipped_snapshots'] = skipped_counts
    
    return sampled_df
        