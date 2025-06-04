import os
import pandas as pd
from fpdf import FPDF

# Create directory if it does not exist
output_dir = 'GAAP_Outputs'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Data for financial statements
balance_sheet_data = {
    'Assets': ['Cash and Cash Equivalents', 'Total Current Assets', 'Non-Current Assets', 'Property, Plant & Equipment', 'Total Assets'],
    'Amount': [40000, 40000, 0, 0, 40000],
    'Liabilities & Owner\'s Equity': ['Accrued Expenses (Salaries)', 'Total Current Liabilities', 'Owner\'s Capital', 'Retained Earnings', 'Total Liabilities & Equity'],
    'Amount2': [6000, 6000, 170000, -136000, 40000]
}

income_statement_data = {
    'Description': ['Revenue', 'Operating Expenses', 'Research and Development', 'General and Administrative', 'Sales and Marketing', 'Total Operating Expenses', 'Operating Income (Loss)', 'Net Income (Loss)'],
    'Amount': [0, '', 20800, 20800, 10400, 52000, -52000, -52000]
}

cash_flows_data = {
    'Description': ['Net Loss', 'Changes in assets and liabilities:', 'Increase in Accrued Expenses (Salaries)', 'Net Cash Used in Operating Activities', 'Cash Flows from Investing Activities', 'Net Cash Used in Investing Activities', 'Owner\'s Capital Contributions', 'Net Cash Provided by Financing Activities', 'Net Increase in Cash and Cash Equivalents', 'Cash and Cash Equivalents at Beginning of Period', 'Cash and Cash Equivalents at End of Period'],
    'Amount': [-52000, '', 6000, -46000, '', 0, 34000, 34000, -12000, 52000, 40000]
}

owners_equity_data = {
    'Description': ['Balance at January 1, 2024', 'Owner\'s Capital Contributions', 'Net Loss for the Period', 'Balance at June 1, 2024'],
    'Owner\'s Capital': [136000, 34000, '', 170000],
    'Retained Earnings': [-104000, '', -52000, -156000],
    'Total Equity': [32000, 34000, -52000, 34000]
}

# Convert data to DataFrames
balance_sheet_df = pd.DataFrame(balance_sheet_data)
income_statement_df = pd.DataFrame(income_statement_data)
cash_flows_df = pd.DataFrame(cash_flows_data)
owners_equity_df = pd.DataFrame(owners_equity_data)

# Format numbers function
def format_currency(value):
    if pd.isna(value) or value == '':
        return ''
    try:
        value = float(value)
        if value < 0:
            return f'(${abs(value):,.0f})'
        else:
            return f'${value:,.0f}'
    except ValueError:
        return str(value)

# Apply the formatting function
for df in [balance_sheet_df, income_statement_df, cash_flows_df, owners_equity_df]:
    for col in df.columns[1:]:
        df[col] = df[col].apply(format_currency)

class PDF(FPDF):
    def header(self):
        # Insert logo at the top left corner
        self.image('logo/logo.png', 10, 8, 33)
        # Set position for the address below the logo
        self.set_xy(10, 43)  # Adjust x, y position based on the logo's size and position
        self.set_font('Arial', '', 10)
        # Add each part of the address on a new line with reduced spacing
        self.cell(0, 5, 'Unit 306', 0, 1, 'L')
        self.cell(0, 5, '57 N St NW', 0, 1, 'L')
        self.cell(0, 5, 'Washington D.C. 20001', 0, 1, 'L')
        self.ln(5)  # Reduced space after the address
        # Add document title
        self.set_xy(10, 65)  # Adjust y position based on the address's position
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, 'Rehani LLC Financial Statements', 0, 1, 'C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def add_table_header(self, col_widths, df, line_height):
        # Table header
        self.set_font('Arial', 'B', 10)
        for col_name, col_width in zip(df.columns, col_widths):
            self.cell(col_width, line_height, col_name, border=1, ln=0, align='C')
        self.ln(line_height)

    def table(self, title, df):
        line_height = self.font_size * 2
        col_widths = self.calculate_col_widths(df)

        # Check if there's enough space for the title and at least one row of the table
        if self.get_y() + line_height * 2 + len(df) * line_height > self.page_break_trigger:
            self.add_page()
        
        self.set_font('Arial', 'B', 12)
        self.cell(0, 10, title, 0, 1, 'C')
        self.ln(5)

        # Add the table header
        self.add_table_header(col_widths, df, line_height)

        self.set_font('Arial', '', 10)
        for index, row in df.iterrows():
            # Check if a new page is needed before adding a new row
            if self.get_y() + line_height + 10 > self.page_break_trigger:
                self.add_page()
                # Print the table title again if starting a new page
                self.set_font('Arial', 'B', 12)
                self.cell(0, 10, title, 0, 1, 'C')
                self.ln(5)
                # Reprint the table header
                self.add_table_header(col_widths, df, line_height)

            for i, col_width in enumerate(col_widths):
                self.cell(col_width, line_height, str(row[i]), border=1, ln=0, align='C')
            self.ln(line_height)
        self.ln(5)

    def calculate_col_widths(self, df):
        page_width = self.w - 2 * self.l_margin
        col_count = len(df.columns)
        col_widths = [page_width / col_count] * col_count
        return col_widths

# Initialize PDF
pdf = PDF()
pdf.add_page()

# Add tables
pdf.table('Balance Sheet as of June 1, 2024', balance_sheet_df)
pdf.table('Income Statement for the Period Ended June 1, 2024', income_statement_df)
pdf.table('Statement of Cash Flows for the Period Ended June 1, 2024', cash_flows_df)
pdf.table('Statement of Owner\'s Equity for the Period Ended June 1, 2024', owners_equity_df)

# Save PDF
pdf_output_path = os.path.join(output_dir, 'Financial_Statements.pdf')
pdf.output(pdf_output_path)

print(f"PDF saved successfully to {pdf_output_path}")
