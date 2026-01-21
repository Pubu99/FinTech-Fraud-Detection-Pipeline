"""
Fraud Analytics Report Generator
Generate visualizations and detailed analytics from fraud detection data
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set style for visualizations
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)


class FraudAnalytics:
    """Analytics and reporting for fraud detection"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.reports_path = './reports'
        os.makedirs(self.reports_path, exist_ok=True)
    
    def get_connection(self):
        """Create database connection"""
        return psycopg2.connect(**self.db_config)
    
    def fetch_fraud_data(self, hours=24):
        """Fetch fraud transactions from database"""
        logger.info(f"Fetching fraud data for last {hours} hours")
        
        conn = self.get_connection()
        query = f"""
            SELECT 
                user_id,
                timestamp,
                merchant_category,
                amount,
                location,
                fraud_reason,
                detected_at
            FROM fraud_transactions
            WHERE timestamp >= NOW() - INTERVAL '{hours} hours'
            ORDER BY timestamp DESC
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        logger.info(f"Fetched {len(df)} fraud transactions")
        return df
    
    def analyze_fraud_by_category(self, df):
        """Analyze fraud patterns by merchant category"""
        if df.empty:
            logger.warning("No data to analyze")
            return None
        
        category_analysis = df.groupby('merchant_category').agg({
            'amount': ['count', 'sum', 'mean', 'max'],
            'user_id': 'nunique'
        }).round(2)
        
        category_analysis.columns = [
            'fraud_count', 'total_amount', 'avg_amount', 
            'max_amount', 'unique_users'
        ]
        
        category_analysis = category_analysis.sort_values('fraud_count', ascending=False)
        
        return category_analysis
    
    def analyze_fraud_by_reason(self, df):
        """Analyze fraud patterns by fraud reason"""
        if df.empty:
            return None
        
        reason_analysis = df.groupby('fraud_reason').agg({
            'amount': ['count', 'sum', 'mean'],
            'user_id': 'nunique'
        }).round(2)
        
        reason_analysis.columns = [
            'fraud_count', 'total_amount', 'avg_amount', 'unique_users'
        ]
        
        return reason_analysis
    
    def analyze_fraud_by_location(self, df):
        """Analyze fraud patterns by location"""
        if df.empty:
            return None
        
        location_analysis = df.groupby('location').agg({
            'amount': ['count', 'sum', 'mean']
        }).round(2)
        
        location_analysis.columns = ['fraud_count', 'total_amount', 'avg_amount']
        location_analysis = location_analysis.sort_values('fraud_count', ascending=False)
        
        return location_analysis
    
    def plot_fraud_by_category(self, df, output_file):
        """Create bar plot for fraud by merchant category"""
        if df.empty:
            return
        
        category_counts = df['merchant_category'].value_counts()
        
        plt.figure(figsize=(12, 6))
        category_counts.plot(kind='bar', color='crimson', alpha=0.7)
        plt.title('Fraud Transactions by Merchant Category', fontsize=16, fontweight='bold')
        plt.xlabel('Merchant Category', fontsize=12)
        plt.ylabel('Number of Fraud Transactions', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved category plot to {output_file}")
    
    def plot_fraud_by_reason(self, df, output_file):
        """Create pie chart for fraud by reason"""
        if df.empty:
            return
        
        reason_counts = df['fraud_reason'].value_counts()
        
        plt.figure(figsize=(10, 8))
        colors = ['#ff6b6b', '#4ecdc4']
        plt.pie(reason_counts.values, labels=reason_counts.index, autopct='%1.1f%%',
                startangle=90, colors=colors, textprops={'fontsize': 12})
        plt.title('Fraud Distribution by Reason', fontsize=16, fontweight='bold')
        plt.axis('equal')
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved reason plot to {output_file}")
    
    def plot_fraud_amount_distribution(self, df, output_file):
        """Create histogram for fraud amount distribution"""
        if df.empty:
            return
        
        plt.figure(figsize=(12, 6))
        plt.hist(df['amount'], bins=30, color='orange', alpha=0.7, edgecolor='black')
        plt.title('Fraud Transaction Amount Distribution', fontsize=16, fontweight='bold')
        plt.xlabel('Transaction Amount (USD)', fontsize=12)
        plt.ylabel('Frequency', fontsize=12)
        plt.axvline(df['amount'].mean(), color='red', linestyle='--', 
                   linewidth=2, label=f'Mean: ${df["amount"].mean():.2f}')
        plt.axvline(df['amount'].median(), color='green', linestyle='--', 
                   linewidth=2, label=f'Median: ${df["amount"].median():.2f}')
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved amount distribution plot to {output_file}")
    
    def plot_fraud_timeline(self, df, output_file):
        """Create timeline plot for fraud transactions"""
        if df.empty:
            return
        
        df_sorted = df.sort_values('timestamp')
        df_sorted['date_hour'] = pd.to_datetime(df_sorted['timestamp']).dt.floor('H')
        
        timeline = df_sorted.groupby('date_hour').size()
        
        plt.figure(figsize=(14, 6))
        timeline.plot(kind='line', color='darkred', linewidth=2, marker='o')
        plt.title('Fraud Transactions Over Time', fontsize=16, fontweight='bold')
        plt.xlabel('Time', fontsize=12)
        plt.ylabel('Number of Fraud Transactions', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved timeline plot to {output_file}")
    
    def generate_comprehensive_report(self, hours=24):
        """Generate comprehensive fraud analytics report"""
        logger.info("Generating comprehensive fraud analytics report")
        
        # Fetch data
        df = self.fetch_fraud_data(hours)
        
        if df.empty:
            logger.warning("No fraud data available for reporting")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Generate analyses
        category_analysis = self.analyze_fraud_by_category(df)
        reason_analysis = self.analyze_fraud_by_reason(df)
        location_analysis = self.analyze_fraud_by_location(df)
        
        # Save CSV reports
        if category_analysis is not None:
            category_file = f"{self.reports_path}/fraud_by_category_{timestamp}.csv"
            category_analysis.to_csv(category_file)
            logger.info(f"Saved category analysis to {category_file}")
        
        if reason_analysis is not None:
            reason_file = f"{self.reports_path}/fraud_by_reason_{timestamp}.csv"
            reason_analysis.to_csv(reason_file)
            logger.info(f"Saved reason analysis to {reason_file}")
        
        if location_analysis is not None:
            location_file = f"{self.reports_path}/fraud_by_location_{timestamp}.csv"
            location_analysis.to_csv(location_file)
            logger.info(f"Saved location analysis to {location_file}")
        
        # Generate visualizations
        self.plot_fraud_by_category(df, 
                                    f"{self.reports_path}/fraud_by_category_{timestamp}.png")
        self.plot_fraud_by_reason(df, 
                                 f"{self.reports_path}/fraud_by_reason_{timestamp}.png")
        self.plot_fraud_amount_distribution(df, 
                                          f"{self.reports_path}/fraud_amount_dist_{timestamp}.png")
        self.plot_fraud_timeline(df, 
                                f"{self.reports_path}/fraud_timeline_{timestamp}.png")
        
        # Generate summary statistics
        summary = {
            'total_fraud_transactions': len(df),
            'total_fraud_amount': df['amount'].sum(),
            'average_fraud_amount': df['amount'].mean(),
            'median_fraud_amount': df['amount'].median(),
            'max_fraud_amount': df['amount'].max(),
            'unique_users_affected': df['user_id'].nunique(),
            'most_common_category': df['merchant_category'].mode()[0] if not df.empty else 'N/A',
            'most_common_reason': df['fraud_reason'].mode()[0] if not df.empty else 'N/A'
        }
        
        summary_df = pd.DataFrame([summary])
        summary_file = f"{self.reports_path}/fraud_summary_{timestamp}.csv"
        summary_df.to_csv(summary_file, index=False)
        
        logger.info(f"Comprehensive report generated successfully")
        logger.info(f"Total Fraud Transactions: {summary['total_fraud_transactions']}")
        logger.info(f"Total Fraud Amount: ${summary['total_fraud_amount']:,.2f}")
        
        return summary


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fraud Analytics Report Generator')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', default='fraud_detection', help='Database name')
    parser.add_argument('--user', default='fraud_admin', help='Database user')
    parser.add_argument('--password', default='fraud_secure_pass', help='Database password')
    parser.add_argument('--hours', type=int, default=24, help='Hours of data to analyze')
    
    args = parser.parse_args()
    
    db_config = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }
    
    analytics = FraudAnalytics(db_config)
    analytics.generate_comprehensive_report(hours=args.hours)
