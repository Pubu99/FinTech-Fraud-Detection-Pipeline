"""
Fraud Analytics Report Generator
Generate visualizations and detailed analytics from fraud detection data
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import psycopg2
from datetime import datetime
import os
import logging
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set style for visualizations
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10


# Formatting helpers
def format_currency(value):
    """Format value as currency string with commas and 2 decimals"""
    if pd.isna(value):
        return "$0.00"
    return f"${float(value):,.2f}"


def format_integer(value):
    """Format integer with commas"""
    if pd.isna(value):
        return "0"
    return f"{int(value):,}"


def currency_formatter(x, pos):
    """Matplotlib formatter for currency on axes"""
    if x >= 1e6:
        return f"${x/1e6:.1f}M"
    elif x >= 1e3:
        return f"${x/1e3:.0f}K"
    else:
        return f"${x:.0f}"


class FraudAnalytics:
    """Analytics and reporting for fraud detection"""
    
    def __init__(self, db_config):
        self.db_config = db_config
        self.reports_path = './reports'
        os.makedirs(self.reports_path, exist_ok=True)
    
    def get_connection(self):
        """Create database connection"""
        try:
            return psycopg2.connect(**self.db_config)
        except psycopg2.OperationalError as exc:
            logger.error(f"Database connection failed: {exc}")
            raise
    
    def fetch_fraud_data(self, hours=24):
        """Fetch fraud transactions from database"""
        logger.info(f"Fetching fraud data for last {hours} hours")

        query = """
            SELECT 
                user_id,
                timestamp,
                merchant_category,
                amount,
                location,
                fraud_reason,
                detected_at
            FROM fraud_transactions
            WHERE timestamp >= NOW() - INTERVAL '%s hours'
            ORDER BY timestamp DESC
        """

        try:
            with self.get_connection() as conn:
                df = pd.read_sql_query(query, conn, params=(hours,))
        except Exception as exc:
            logger.error(f"Failed to fetch fraud data: {exc}")
            raise

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

    def analyze_fraud_by_hour(self, df):
        """Analyze fraud activity by hour of day"""
        if df.empty:
            return None

        df_local = df.copy()
        df_local['hour'] = pd.to_datetime(df_local['timestamp']).dt.hour

        hour_analysis = df_local.groupby('hour').agg({
            'amount': ['count', 'sum', 'mean']
        }).round(2)

        hour_analysis.columns = ['fraud_count', 'total_amount', 'avg_amount']
        return hour_analysis.sort_index()
    
    def plot_fraud_by_category(self, df, output_file):
        """Create bar plot for fraud by merchant category"""
        if df.empty:
            return
        
        category_counts = df['merchant_category'].value_counts().head(10)
        
        fig, ax = plt.subplots(figsize=(14, 7))
        bars = ax.bar(range(len(category_counts)), category_counts.values, color='#d73027', alpha=0.85, edgecolor='black', linewidth=1.2)
        
        # Add value labels on bars
        for bar, val in zip(bars, category_counts.values):
            ax.text(bar.get_x() + bar.get_width()/2, val + 20, format_integer(val), 
                   ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        ax.set_xticks(range(len(category_counts)))
        ax.set_xticklabels(category_counts.index, rotation=45, ha='right')
        ax.set_title('Top 10 Fraud Transactions by Merchant Category', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Merchant Category', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Fraud Transactions', fontsize=12, fontweight='bold')
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_integer(x)))
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        sns.despine(ax=ax)
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved category plot to {output_file}")
    
    def plot_fraud_by_reason(self, df, output_file):
        """Create pie chart for fraud by reason"""
        if df.empty:
            return
        
        reason_counts = df['fraud_reason'].value_counts()
        
        fig, ax = plt.subplots(figsize=(12, 8))
        colors_palette = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00']
        wedges, texts, autotexts = ax.pie(reason_counts.values, labels=reason_counts.index, 
                                           autopct='%1.1f%%', startangle=90, 
                                           colors=colors_palette, textprops={'fontsize': 11, 'fontweight': 'bold'},
                                           explode=[0.05] * len(reason_counts),
                                           shadow=True)
        
        # Enhance autotext
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(12)
            autotext.set_fontweight('bold')
        
        ax.set_title('Fraud Distribution by Reason', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved reason plot to {output_file}")
    
    def plot_fraud_amount_distribution(self, df, output_file):
        """Create histogram with KDE for fraud amount distribution"""
        if df.empty:
            return
        
        fig, ax = plt.subplots(figsize=(14, 7))
        
        # Use seaborn histplot with KDE
        sns.histplot(data=df, x='amount', bins=40, color='#ff7f00', alpha=0.7, 
                    kde=True, line_kws={'linewidth': 2.5, 'color': '#d73027'}, ax=ax)
        
        # Add mean and median lines
        mean_val = df['amount'].mean()
        median_val = df['amount'].median()
        ax.axvline(mean_val, color='#1f77b4', linestyle='--', linewidth=2.5, label=f'Mean: {format_currency(mean_val)}')
        ax.axvline(median_val, color='#2ca02c', linestyle='--', linewidth=2.5, label=f'Median: {format_currency(median_val)}')
        
        ax.set_title('Fraud Transaction Amount Distribution', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Transaction Amount (USD)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(currency_formatter))
        ax.legend(fontsize=11, loc='upper right')
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        sns.despine(ax=ax)
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved amount distribution plot to {output_file}")
    
    def plot_fraud_timeline(self, df, output_file):
        """Create timeline plot for fraud transactions with fill area"""
        if df.empty:
            return
        
        df_sorted = df.sort_values('timestamp')
        df_sorted['date_hour'] = pd.to_datetime(df_sorted['timestamp']).dt.floor('H')
        
        timeline = df_sorted.groupby('date_hour').size()
        
        fig, ax = plt.subplots(figsize=(16, 7))
        
        # Plot line with fill
        ax.plot(timeline.index, timeline.values, color='#d73027', linewidth=3, marker='o', markersize=6, label='Fraud Count')
        ax.fill_between(timeline.index, timeline.values, alpha=0.3, color='#d73027')
        
        ax.set_title('Fraud Transactions Over Time', fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Time', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Fraud Transactions', fontsize=12, fontweight='bold')
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format_integer(x)))
        ax.legend(fontsize=11, loc='upper left')
        ax.grid(True, alpha=0.3, linestyle='--')
        sns.despine(ax=ax)
        plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved timeline plot to {output_file}")

    def build_summary_stats(self, df):
        """Build summary statistics for report"""
        if df.empty:
            return None

        summary = {
            'total_fraud_transactions': len(df),
            'total_fraud_amount': df['amount'].sum(),
            'average_fraud_amount': df['amount'].mean(),
            'median_fraud_amount': df['amount'].median(),
            'max_fraud_amount': df['amount'].max(),
            'unique_users_affected': df['user_id'].nunique(),
            'most_common_category': df['merchant_category'].mode()[0],
            'most_common_reason': df['fraud_reason'].mode()[0]
        }

        return summary

    def generate_pdf_report(self, df, summary, analyses, plot_files, output_file):
        """Generate a professional PDF report using ReportLab"""
        if df.empty:
            return
        try:
            # Create PDF document
            doc = SimpleDocTemplate(output_file, pagesize=letter, 
                                   topMargin=0.5*inch, bottomMargin=0.5*inch,
                                   leftMargin=0.5*inch, rightMargin=0.5*inch)
            story = []
            styles = getSampleStyleSheet()
            
            # Custom styles
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=28,
                textColor=colors.HexColor('#d73027'),
                spaceAfter=12,
                alignment=TA_CENTER,
                fontName='Helvetica-Bold'
            )
            heading_style = ParagraphStyle(
                'CustomHeading',
                parent=styles['Heading2'],
                fontSize=14,
                textColor=colors.HexColor('#1a1a1a'),
                spaceAfter=12,
                spaceBefore=12,
                fontName='Helvetica-Bold'
            )
            
            # Cover page
            story.append(Spacer(1, 1.5*inch))
            story.append(Paragraph("Fraud Analytics Report", title_style))
            story.append(Spacer(1, 0.3*inch))
            story.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                                  ParagraphStyle('subtitle', parent=styles['Normal'], fontSize=12, alignment=TA_CENTER)))
            story.append(Paragraph(f"Data Window: Last {analyses['hours']} hours", 
                                  ParagraphStyle('subtitle', parent=styles['Normal'], fontSize=12, alignment=TA_CENTER)))
            story.append(PageBreak())
            
            # Summary statistics table
            story.append(Paragraph("Summary Statistics", heading_style))
            summary_data = [['Total Txns', 'Total Amount', 'Avg Amount', 'Median Amount', 'Max Amount', 'Unique Users', 'Top Category', 'Top Reason']]
            summary_data.append([
                format_integer(summary['total_fraud_transactions']),
                format_currency(summary['total_fraud_amount']),
                format_currency(summary['average_fraud_amount']),
                format_currency(summary['median_fraud_amount']),
                format_currency(summary['max_fraud_amount']),
                format_integer(summary['unique_users_affected']),
                str(summary['most_common_category']),
                str(summary['most_common_reason'])
            ])
            
            summary_table = Table(summary_data, colWidths=[0.95*inch]*8)
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#d73027')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('FONTSIZE', (0, 1), (-1, 1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('TOPPADDING', (0, 0), (-1, 0), 12),
                ('TOPPADDING', (0, 1), (-1, 1), 10),
                ('BOTTOMPADDING', (0, 1), (-1, 1), 10),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f0f0')])
            ]))
            story.append(summary_table)
            story.append(Spacer(1, 0.3*inch))
            story.append(PageBreak())
            
            # Analysis tables
            for table_title, analysis_df in analyses['tables']:
                if analysis_df is None or analysis_df.empty:
                    continue
                    
                story.append(Paragraph(table_title, heading_style))
                table_df = analysis_df.head(12).copy()
                
                # Format data
                formatted_data = [['Index'] + list(table_df.columns)]
                for idx, row in table_df.iterrows():
                    formatted_row = [str(idx)]
                    for col_name, val in row.items():
                        if 'amount' in col_name.lower():
                            formatted_row.append(format_currency(val))
                        elif isinstance(val, (int, float)) and val > 100:
                            formatted_row.append(format_integer(val))
                        else:
                            formatted_row.append(f"{val:.2f}" if isinstance(val, float) else str(val))
                    formatted_data.append(formatted_row)
                
                col_width = 7.5*inch / len(formatted_data[0])
                data_table = Table(formatted_data, colWidths=[col_width]*len(formatted_data[0]))
                data_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f77b4')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 9),
                    ('FONTSIZE', (0, 1), (-1, -1), 8),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                    ('TOPPADDING', (0, 0), (-1, 0), 10),
                    ('TOPPADDING', (0, 1), (-1, -1), 6),
                    ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f8f8')])
                ]))
                story.append(data_table)
                story.append(Spacer(1, 0.2*inch))
                story.append(PageBreak())
            
            # Add charts
            for plot_path in plot_files:
                if not os.path.exists(plot_path):
                    continue
                try:
                    img = Image(plot_path, width=7*inch, height=4*inch)
                    story.append(img)
                    story.append(Spacer(1, 0.2*inch))
                    story.append(PageBreak())
                except Exception as e:
                    logger.warning(f"Could not embed image {plot_path}: {e}")
            
            # Build PDF
            doc.build(story)
            logger.info(f"PDF report generated successfully: {output_file}")
            
        except Exception as exc:
            logger.error(f"Failed to generate PDF report: {exc}")
            raise
    
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
        hour_analysis = self.analyze_fraud_by_hour(df)
        
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
        summary = self.build_summary_stats(df)
        
        summary_df = pd.DataFrame([summary])
        summary_file = f"{self.reports_path}/fraud_summary_{timestamp}.csv"
        summary_df.to_csv(summary_file, index=False)
        
        logger.info(f"Comprehensive report generated successfully")
        logger.info(f"Total Fraud Transactions: {summary['total_fraud_transactions']}")
        logger.info(f"Total Fraud Amount: ${summary['total_fraud_amount']:,.2f}")
        
        # Generate PDF report
        pdf_file = f"{self.reports_path}/fraud_report_{timestamp}.pdf"
        analyses = {
            'hours': hours,
            'tables': [
                ('Fraud by Category (Top 15)', category_analysis),
                ('Fraud by Reason', reason_analysis),
                ('Fraud by Location (Top 15)', location_analysis),
                ('Fraud by Hour', hour_analysis)
            ]
        }
        plot_files = [
            f"{self.reports_path}/fraud_by_category_{timestamp}.png",
            f"{self.reports_path}/fraud_by_reason_{timestamp}.png",
            f"{self.reports_path}/fraud_amount_dist_{timestamp}.png",
            f"{self.reports_path}/fraud_timeline_{timestamp}.png"
        ]
        self.generate_pdf_report(df, summary, analyses, plot_files, pdf_file)
        logger.info(f"Saved PDF report to {pdf_file}")

        return summary


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fraud Analytics Report Generator')
    parser.add_argument('--host', default='localhost', help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', default='fraud_detection', help='Database name')
    parser.add_argument('--user', default='fraud_admin', help='Database user')
    parser.add_argument('--password', default=os.getenv('DB_PASSWORD'), help='Database password')
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
