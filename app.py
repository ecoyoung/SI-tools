import streamlit as st
import pandas as pd
import numpy as np
import re
from io import BytesIO
import xlsxwriter
import zipfile

# 设置页面配置
st.set_page_config(
    page_title="关键词品牌匹配工具",
    page_icon="🔍",
    layout="wide"
)

# 初始化session state
if 'custom_rules' not in st.session_state:
    st.session_state.custom_rules = pd.DataFrame(columns=['品牌名称', '匹配关键词'])
if 'product_data' not in st.session_state:
    st.session_state.product_data = None
if 'brand_data' not in st.session_state:
    st.session_state.brand_data = None
if 'matched_results' not in st.session_state:
    st.session_state.matched_results = None

def process_product_data(df):
    """处理产品数据，计算排名和累计占比"""
    # 数据清洗和类型转换
    df_clean = df.copy()
    
    # 检查必要的列是否存在
    required_columns = ['关键词', '月搜索量']
    missing_columns = [col for col in required_columns if col not in df_clean.columns]
    if missing_columns:
        st.error(f"❌ 缺少必要的列：{missing_columns}")
        return None
    
    # 清理月搜索量列
    df_clean['月搜索量'] = pd.to_numeric(df_clean['月搜索量'], errors='coerce')
    
    # 移除月搜索量为NaN或0的行
    df_clean = df_clean.dropna(subset=['月搜索量'])
    df_clean = df_clean[df_clean['月搜索量'] > 0]
    
    if df_clean.empty:
        st.error("❌ 没有有效的月搜索量数据")
        return None
    
    # 排序和计算
    df_sorted = df_clean.sort_values('月搜索量', ascending=False).reset_index(drop=True)
    df_sorted['Rank'] = range(1, len(df_sorted) + 1)
    df_sorted['月搜索量累计和'] = df_sorted['月搜索量'].cumsum()
    df_sorted['月搜索量累计占比'] = df_sorted['月搜索量累计和'] / df_sorted['月搜索量'].sum()
    return df_sorted

def match_brands(product_df, brand_df, custom_rules_df):
    """执行品牌匹配逻辑"""
    # 数据验证
    if product_df is None or product_df.empty:
        st.error("❌ 产品数据为空")
        return None
    
    if brand_df is None or brand_df.empty:
        st.error("❌ 品牌数据为空")
        return None
    
    # 准备数据
    result_df = product_df[['关键词', '月搜索量']].copy()
    result_df['keyword_lower'] = result_df['关键词'].astype(str).str.lower()
    
    # 准备品牌词（转小写）
    brand_list = brand_df['品牌名称'].astype(str).str.lower().tolist()
    
    # 准备手动规则
    manual_map = {}
    if not custom_rules_df.empty:
        for _, row in custom_rules_df.iterrows():
            brand_name = str(row['品牌名称'])
            keywords = [kw.strip().lower() for kw in str(row['匹配关键词']).split(',') if kw.strip()]
            for kw in keywords:
                manual_map[kw] = brand_name
    
    # 执行匹配
    result_df['品牌名称'] = None
    result_df['品牌'] = None
    result_df['词性'] = 'Non-Branded KWs'
    
    for idx, row in result_df.iterrows():
        keyword_lower = row['keyword_lower']
        matched_brand = None
        matched_term = None
        
        # 1. 优先检查手动规则（精准匹配，整词匹配）
        for manual_kw, manual_brand in manual_map.items():
            if manual_kw:
                # 使用正则整词匹配，忽略大小写
                pattern = r'(?<!\w)' + re.escape(manual_kw) + r'(?!\w)'
                if re.search(pattern, keyword_lower, re.IGNORECASE):
                    matched_brand = manual_brand
                    matched_term = manual_kw
                    break
        
        # 2. 如果手动规则没匹配到，检查品牌词库（精准匹配，整词匹配）
        if not matched_brand:
            for brand in brand_list:
                if brand:
                    pattern = r'(?<!\w)' + re.escape(brand) + r'(?!\w)'
                    if re.search(pattern, keyword_lower, re.IGNORECASE):
                        # 找到对应的原始品牌名称
                        original_brand = brand_df[brand_df['品牌名称'].str.lower() == brand]['品牌名称'].iloc[0]
                        matched_brand = original_brand
                        matched_term = brand
                        break
        
        # 3. 更新结果
        if matched_brand:
            result_df.at[idx, '品牌名称'] = matched_brand
            result_df.at[idx, '品牌'] = matched_term
            result_df.at[idx, '词性'] = 'Branded KWs'
    
    # 添加特性参数列
    result_df['特性参数'] = None
    
    # 清理临时列
    result_df = result_df.drop('keyword_lower', axis=1)
    
    return result_df

def create_download_file(df):
    """创建Excel下载文件"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='品牌匹配结果', index=False)
    return output.getvalue()

# 主界面
st.title("🔍 关键词品牌匹配工具")
st.markdown("**开发维护：IDC部门**")

# 侧边栏
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/9/9c/Anker_logo.svg", width=200)
    
    st.header("📁 文件上传")
    
    # 产品关键词文件上传
    product_file = st.file_uploader(
        "上传产品关键词文件",
        type=['xlsx', 'xls'],
        help="请上传包含关键词和月搜索量的Excel文件"
    )
    
    # 品牌词文件上传
    brand_file = st.file_uploader(
        "上传欧鹭品牌词数据文件",
        type=['xlsx', 'xls'],
        help="请上传包含品牌名称的Excel文件"
    )
    
    st.header("⚙️ 手动规则配置")
    
    # 手动添加品牌规则
    with st.form("add_rule_form"):
        custom_brand = st.text_input("归属品牌名")
        custom_keywords = st.text_input("匹配关键词（英文逗号分隔）")
        submitted = st.form_submit_button("添加品牌规则")
        
        if submitted and custom_brand and custom_keywords:
            new_rule = pd.DataFrame({
                '品牌名称': [custom_brand],
                '匹配关键词': [custom_keywords]
            })
            st.session_state.custom_rules = pd.concat([st.session_state.custom_rules, new_rule], ignore_index=True)
            st.success("规则添加成功！")
    
    # 显示自定义规则
    if not st.session_state.custom_rules.empty:
        st.subheader("当前规则")
        st.dataframe(st.session_state.custom_rules, hide_index=True)
        
        if st.button("清空所有规则"):
            st.session_state.custom_rules = pd.DataFrame(columns=['品牌名称', '匹配关键词'])
            st.rerun()

# 处理文件上传
if product_file:
    try:
        # 跳过前两行读取Excel
        df = pd.read_excel(product_file, skiprows=2)
        
        # 显示原始数据的列名，帮助用户了解数据结构
        st.info(f"📋 检测到的列名：{list(df.columns)}")
        
        # 检查是否包含必要的列
        if '关键词' not in df.columns or '月搜索量' not in df.columns:
            st.error("❌ 文件必须包含'关键词'和'月搜索量'列")
            st.info("💡 请确保Excel文件包含正确的列名")
        else:
            st.session_state.product_data = df
            st.success("✅ 产品关键词文件上传成功！")
            
    except Exception as e:
        st.error(f"❌ 产品关键词文件读取失败：{str(e)}")
        st.info("💡 请确保文件是有效的Excel格式(.xlsx或.xls)")

if brand_file:
    try:
        brand_df = pd.read_excel(brand_file)
        
        # 显示原始数据的列名
        st.info(f"📋 检测到的列名：{list(brand_df.columns)}")
        
        # 检查是否包含必要的列
        if '品牌名称' not in brand_df.columns:
            st.error("❌ 文件必须包含'品牌名称'列")
            st.info("💡 请确保Excel文件包含正确的列名")
        else:
            # 过滤空值并去重
            st.session_state.brand_data = brand_df.dropna(subset=['品牌名称']).drop_duplicates(subset=['品牌名称']).reset_index(drop=True)
            st.success("✅ 品牌词数据文件上传成功！")
            
    except Exception as e:
        st.error(f"❌ 品牌词数据文件读取失败：{str(e)}")
        st.info("💡 请确保文件是有效的Excel格式(.xlsx或.xls)")

# 主内容区域
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 产品关键词排名", "🏷️ 品牌词数据", "🎯 品牌匹配结果", "🔧 ASIN去重工具", "📂 批量合并ZIP文件", "📁 批量合并文件"])

with tab1:
    st.header("产品关键词排名")
    
    if st.session_state.product_data is not None:
        processed_data = process_product_data(st.session_state.product_data)
        
        if processed_data is not None:
            # 显示60%累计占比的排名提示
            percent_60_data = processed_data[processed_data['月搜索量累计占比'] >= 0.6]
            if not percent_60_data.empty:
                rank_60_percent = percent_60_data.iloc[0]['Rank']
                st.info(f"📈 当月搜索量累计占比达到 60% 时的关键词排名为：**{int(rank_60_percent)}**")
            else:
                st.info("📈 所有关键词的累计占比都未达到 60%")
            
            # 显示数据表格
            display_columns = [col for col in processed_data.columns if col not in ['月搜索量累计占比', '月搜索量累计和']]
            st.dataframe(
                processed_data[display_columns],
                hide_index=True,
                use_container_width=True
            )
            
            # 显示统计信息
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("总关键词数", len(processed_data))
            with col2:
                st.metric("总月搜索量", f"{processed_data['月搜索量'].sum():,}")
            with col3:
                st.metric("平均月搜索量", f"{processed_data['月搜索量'].mean():.0f}")
        else:
            st.error("❌ 数据处理失败，请检查文件格式")
    else:
        st.info("请先上传产品关键词文件")

with tab2:
    st.header("品牌词数据")
    
    if st.session_state.brand_data is not None:
        st.dataframe(
            st.session_state.brand_data[['品牌名称']],
            hide_index=True,
            use_container_width=True
        )
        
        st.metric("品牌总数", len(st.session_state.brand_data))
    else:
        st.info("请先上传品牌词数据文件")

with tab3:
    st.header("品牌匹配结果")
    
    # 运行匹配按钮
    if st.button("🚀 运行品牌匹配", type="primary", use_container_width=True):
        if st.session_state.product_data is not None and st.session_state.brand_data is not None:
            with st.spinner("正在执行品牌匹配..."):
                st.session_state.matched_results = match_brands(
                    st.session_state.product_data,
                    st.session_state.brand_data,
                    st.session_state.custom_rules
                )
            st.success("✅ 品牌匹配完成！")
        else:
            st.error("❌ 请先上传产品关键词文件和品牌词数据文件")
    
    # 显示匹配结果
    if st.session_state.matched_results is not None:
        # 统计信息
        total_keywords = len(st.session_state.matched_results)
        branded_keywords = len(st.session_state.matched_results[st.session_state.matched_results['词性'] == 'Branded KWs'])
        non_branded_keywords = total_keywords - branded_keywords
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("总关键词", total_keywords)
        with col2:
            st.metric("品牌词", branded_keywords)
        with col3:
            st.metric("非品牌词", non_branded_keywords)
        with col4:
            st.metric("品牌词占比", f"{branded_keywords/total_keywords*100:.1f}%")
        
        # 筛选选项
        col1, col2 = st.columns(2)
        with col1:
            word_type_filter = st.selectbox(
                "筛选词性",
                options=["全部", "Branded KWs", "Non-Branded KWs"]
            )
        with col2:
            brand_filter = st.selectbox(
                "筛选品牌",
                options=["全部"] + list(st.session_state.matched_results['品牌名称'].dropna().unique())
            )
        
        # 应用筛选
        filtered_results = st.session_state.matched_results.copy()
        if word_type_filter != "全部":
            filtered_results = filtered_results[filtered_results['词性'] == word_type_filter]
        if brand_filter != "全部":
            filtered_results = filtered_results[filtered_results['品牌名称'] == brand_filter]
        
        # 显示筛选后的数据
        st.dataframe(
            filtered_results,
            hide_index=True,
            use_container_width=True
        )
        
        # 下载按钮
        if st.button("📥 下载品牌匹配结果", use_container_width=True):
            excel_file = create_download_file(st.session_state.matched_results)
            st.download_button(
                label="下载Excel文件",
                data=excel_file,
                file_name=f"品牌匹配结果_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
    else:
        st.info("请点击'运行品牌匹配'按钮开始匹配")

with tab4:
    st.header("🔧 ASIN去重工具")
    
    # ASIN输入区域
    st.subheader("📝 输入ASIN")
    asin_input = st.text_area(
        "请输入ASIN（支持换行、空格、逗号等分隔符）",
        height=200,
        placeholder="请输入ASIN，例如：\nB08N5WRWNW\nB08N5KWB9H\nB08N5KWB9H\nB08N5WRWNW"
    )
    
    # 去重按钮
    if st.button("🔄 执行去重", type="primary", use_container_width=True):
        if asin_input.strip():
            # 处理输入的ASIN
            # 支持多种分隔符：换行、空格、逗号、分号、制表符
            asin_list = re.split(r'[\n\s,;\t]+', asin_input.strip())
            # 过滤空字符串和清理ASIN格式
            asin_list = [asin.strip().upper() for asin in asin_list if asin.strip()]
            
            # 记录去重前的数量
            original_count = len(asin_list)
            
            # 去重
            unique_asins = list(dict.fromkeys(asin_list))  # 保持顺序的去重
            
            # 记录去重后的数量
            unique_count = len(unique_asins)
            
            # 保存结果到session state
            st.session_state.asin_results = {
                'original_count': original_count,
                'unique_count': unique_count,
                'unique_asins': unique_asins
            }
            
            st.success(f"✅ 去重完成！去重前：{original_count}个，去重后：{unique_count}个")
        else:
            st.error("❌ 请输入ASIN")
    
    # 显示去重结果
    if hasattr(st.session_state, 'asin_results') and st.session_state.asin_results:
        results = st.session_state.asin_results
        
        st.subheader("📊 去重结果")
        
        # 统计信息
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("去重前数量", results['original_count'])
        with col2:
            st.metric("去重后数量", results['unique_count'])
        with col3:
            removed_count = results['original_count'] - results['unique_count']
            st.metric("重复数量", removed_count)
        
        # 显示去重后的ASIN
        st.subheader("🎯 去重后的ASIN")
        
        # 将ASIN用空格连接
        asin_text = ' '.join(results['unique_asins'])
        
        # 创建可复制的文本框
        st.text_area(
            "去重后的ASIN（用空格分隔）",
            value=asin_text,
            height=100,
            help="点击文本框内的内容可以全选复制"
        )
        
        # 复制按钮
        if st.button("📋 复制到剪贴板", use_container_width=True):
            st.write("💡 请手动选择上方文本框中的内容并复制")
        
        # 显示详细的ASIN列表
        st.subheader("📋 详细列表")
        st.dataframe(
            pd.DataFrame({
                '序号': range(1, len(results['unique_asins']) + 1),
                'ASIN': results['unique_asins']
            }),
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("请在输入框中输入ASIN，然后点击'执行去重'按钮")

with tab5:
    st.header("📂 批量合并ZIP文件")
    st.markdown("上传多个zip文件（每个zip内仅包含一个csv或xlsx文件），自动合并并用zip文件名作为'时间'列")

    uploaded_zips = st.file_uploader(
        "请上传zip文件（可多选）",
        type=["zip"],
        accept_multiple_files=True
    )

    if uploaded_zips:
        all_data = []
        error_files = []
        for zip_file in uploaded_zips:
            try:
                with zipfile.ZipFile(zip_file) as z:
                    # 只处理第一个csv或xlsx文件
                    file_list = [f for f in z.namelist() if f.lower().endswith(('.csv', '.xlsx'))]
                    if not file_list:
                        error_files.append(zip_file.name)
                        continue
                    file_name = file_list[0]
                    with z.open(file_name) as f:
                        if file_name.lower().endswith('.csv'):
                            df = pd.read_csv(f)
                        else:
                            df = pd.read_excel(f)
                    # 增加"时间"列，取zip文件名（去掉扩展名）
                    df['时间'] = zip_file.name.rsplit('.', 1)[0]
                    all_data.append(df)
            except Exception as e:
                error_files.append(zip_file.name)
        if all_data:
            merged_df = pd.concat(all_data, ignore_index=True)
            st.success(f"合并成功，共 {len(all_data)} 个zip文件，合计 {len(merged_df)} 行数据。")
            st.dataframe(merged_df, use_container_width=True)
            # 下载按钮
            towrite = BytesIO()
            merged_df.to_excel(towrite, index=False)
            towrite.seek(0)
            st.download_button(
                label="下载合并结果Excel",
                data=towrite,
                file_name="合并结果.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        else:
            st.warning("没有成功合并的数据，请检查上传的zip文件格式。")
        if error_files:
            st.error(f"以下zip文件处理失败或未找到csv/xlsx文件：{', '.join(error_files)}")
    else:
        st.info("请上传需要合并的zip文件。")

with tab6:
    st.header("📁 批量合并文件")
    st.markdown("上传多个xlsx或csv文件，自动合并并用文件名作为'时间'列放在第一列")

    uploaded_files = st.file_uploader(
        "请上传xlsx或csv文件（可多选）",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True
    )

    if uploaded_files:
        all_data = []
        error_files = []
        
        for file in uploaded_files:
            try:
                # 根据文件扩展名读取数据
                if file.name.lower().endswith('.csv'):
                    df = pd.read_csv(file)
                else:  # xlsx or xls
                    df = pd.read_excel(file)
                
                # 获取文件名（去掉扩展名）作为时间列
                time_column = file.name.rsplit('.', 1)[0]
                
                # 添加时间列到第一列
                df.insert(0, '时间', time_column)
                
                all_data.append(df)
                st.success(f"✅ 成功读取文件：{file.name}")
                
            except Exception as e:
                error_files.append(file.name)
                st.error(f"❌ 读取文件失败：{file.name} - {str(e)}")
        
        if all_data:
            # 合并所有数据
            merged_df = pd.concat(all_data, ignore_index=True)
            
            st.success(f"🎉 合并成功！共处理 {len(all_data)} 个文件，合计 {len(merged_df)} 行数据")
            
            # 调试信息
            st.info(f"调试：all_data长度 = {len(all_data)}, merged_df形状 = {merged_df.shape}")
            
            # 显示统计信息
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("处理文件数", len(all_data))
            with col2:
                st.metric("总行数", len(merged_df))
            with col3:
                st.metric("时间列唯一值", merged_df['时间'].nunique())
            
            # 显示合并后的数据
            st.subheader("📊 合并结果预览")
            st.dataframe(merged_df, use_container_width=True)
            
            # 显示时间列统计
            st.subheader("📅 时间列统计")
            time_stats = merged_df['时间'].value_counts().reset_index()
            time_stats.columns = ['时间', '行数']
            st.dataframe(time_stats, hide_index=True, use_container_width=True)
            
            # 下载按钮
            st.subheader("📥 下载合并结果")
            
            # 调试信息
            st.info("正在生成下载文件...")
            
            # Excel下载
            excel_output = BytesIO()
            merged_df.to_excel(excel_output, index=False)
            excel_output.seek(0)
            
            st.download_button(
                label="📊 下载Excel文件",
                data=excel_output.getvalue(),
                file_name=f"批量合并结果_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
            # CSV下载
            csv_output = BytesIO()
            merged_df.to_csv(csv_output, index=False, encoding='utf-8-sig')
            csv_output.seek(0)
            
            st.download_button(
                label="📄 下载CSV文件",
                data=csv_output.getvalue(),
                file_name=f"批量合并结果_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.warning("⚠️ 没有成功合并的数据，请检查上传的文件格式")
        
        if error_files:
            st.error(f"❌ 以下文件处理失败：{', '.join(error_files)}")
    else:
        st.info("📁 请上传需要合并的xlsx或csv文件")

# 页脚
st.markdown("---")
st.markdown("💡 **使用说明**：")
st.markdown("""
1. 上传产品关键词Excel文件（需包含'关键词'和'月搜索量'列）
2. 上传品牌词Excel文件（需包含'品牌名称'列）
3. 可选：添加手动匹配规则
4. 点击'运行品牌匹配'执行匹配
5. 查看结果并下载Excel文件
""")