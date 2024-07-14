
import pandas as pd
import numpy as np

# Define the transform_data function for data transformation
def transform_data(ti):
        from sklearn.preprocessing import StandardScaler  # For data scaling
        from sklearn.cluster import KMeans  # For KMeans clustering

        # Extract data from XCom
        extracted_data = ti.xcom_pull(task_ids='extract_from_mysql')
        columns = ['index', 'InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
        df = pd.DataFrame(extracted_data, columns=columns)

        # Data preprocessing steps
        df['Quantity'] = abs(df['Quantity'])
        df = df[df['UnitPrice'] > 0]
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        df['month_index'] = df['InvoiceDate'].dt.month * 0.01 + df['InvoiceDate'].dt.year
        df['revenue'] = df['UnitPrice'] * df['Quantity']

        # Aggregating data by CustomerID
        result = df.groupby(['CustomerID']).agg(
            first_transactions=pd.NamedAgg(column='InvoiceDate', aggfunc='min'),
            last_transactions=pd.NamedAgg(column='InvoiceDate', aggfunc='max'),
            Total_revenue=pd.NamedAgg(column='revenue', aggfunc='sum'),
            Number_Transactions=pd.NamedAgg(column='CustomerID', aggfunc='count'),
            nunique_month_index=pd.NamedAgg(column='month_index', aggfunc=pd.Series.nunique)
        ).reset_index()

        # Calculating additional features
        max_date = df['InvoiceDate'].max()
        result['first-last'] =(result['last_transactions'].dt.month +  result['last_transactions'].dt.year *12 ) - (result['first_transactions'].dt.month + result['first_transactions'].dt.year * 12) + 1
        result['User Age'] =(max_date.month +  max_date.year *12 ) - (result['first_transactions'].dt.month + result['first_transactions'].dt.year * 12) + 1
        result['recency'] = result['User Age'] - result['first-last'] + 1

        # Log transformation
        result['Recency-Log'] = np.log(result['recency'])
        result['Monetary-Log'] = np.log(result['Total_revenue'])
        result['Frequency-Log'] = np.log(result['Number_Transactions'])
        
        # Scaling scores to a range of 0 to 5
        max_GTV = max(result['Monetary-Log'])
        min_GTV = min(result['Monetary-Log'])
        max_TRX = max(result['Frequency-Log'])
        min_TRX = min(result['Frequency-Log'])
        min_recency = min(result['Recency-Log'])
        max_recency = max(result['Recency-Log'])

        result['Recency_Score-Log'] = 5 * (result['Recency-Log'] - min_recency) / (max_recency - min_recency)
        result['Frequency_Score-Log'] = 5 * (result['Frequency-Log'] - min_TRX) / (max_TRX - min_TRX)
        result['Monetary_Score-Log'] = 5 * (result['Monetary-Log'] - min_GTV) / (max_GTV - min_GTV)

        # Prepare data for clustering
        result_score = result[['Recency_Score-Log', 'Frequency_Score-Log', 'Monetary_Score-Log']]
        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(result_score)

        # Apply KMeans clustering
        k = 4  # Number of clusters
        kmeans = KMeans(n_clusters=k, random_state=42).fit(df_scaled)
        result_score['cluster_label'] = kmeans.fit_predict(df_scaled)

        # Final dataframe preparation
        df_final = pd.concat([result, result_score[['cluster_label']]], axis=1)
        df_final = df_final[['CustomerID', 'Total_revenue', 'Number_Transactions', 'recency', 'cluster_label']] 

        # Function to assign cluster labels
        grouped_df = df_final.groupby('cluster_label', as_index=False).agg({'Total_revenue': 'mean'})
        sorted_df = grouped_df.sort_values(by='Total_revenue', ascending=False).reset_index(drop=True)

        # Assign new cluster labels
        label_mapping = {label: new_label for label, new_label in zip(sorted_df['cluster_label'], ['Champion', 'Potential', 'Sleep', 'Lost'])}

        # Create 'new_label' column in the original DataFrame
        df_final['cluster_label_new'] = df_final['cluster_label'].map(label_mapping)
     
        # Push the transformed data to XCom
        ti.xcom_push(key='transformed_data', value=df_final.to_dict('records'))
