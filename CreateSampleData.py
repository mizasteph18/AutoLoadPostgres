#!/usr/bin/env python3
"""
Sample data creation script for PostgreSQL Data Loader testing
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

def create_sample_data():
    """Create sample data files for testing"""
    
    # Create date strings for the last 4 weeks
    today = datetime.today()
    dates = [today - timedelta(days=7 * i) for i in range(4)]
    
    for dt in dates:
        date_str = dt.strftime("%Y%m%d")
        
        # Sales data (CSV)
        sales_file = Path("inputs/sales_data") / f"sales_{date_str}.csv"
        if not sales_file.exists():
            sales_data = pd.DataFrame({
                "OrderID": range(1, 101),
                "Customer": np.random.choice(["Alice", "Bob", "Charlie", "David", "Eve"], 100),
                "Amount": np.round(np.random.uniform(10, 500, 100), 2)
            })
            sales_data.to_csv(sales_file, index=False)
            print(f"Created: {sales_file}")
        
        # Inventory data (Excel with multiple sheets)
        inventory_file = Path("inputs/inventory_data") / f"inventory_{date_str}.xlsx"
        if not inventory_file.exists():
            with pd.ExcelWriter(inventory_file, engine='openpyxl') as writer:
                # Sheet 1
                sheet1_data = pd.DataFrame({
                    "ItemID": range(101, 151),
                    "ItemName": [f"Widget_{i}" for i in range(101, 151)],
                    "Stock": np.random.randint(0, 100, 50)
                })
                sheet1_data.to_excel(writer, sheet_name='Sheet1', index=False)
                
                # Sheet 2  
                sheet2_data = pd.DataFrame({
                    "ItemID": range(201, 251),
                    "ItemName": [f"Gadget_{i}" for i in range(201, 251)],
                    "Stock": np.random.randint(0, 200, 50)
                })
                sheet2_data.to_excel(writer, sheet_name='Sheet2', index=False)
            print(f"Created: {inventory_file}")
        
        # Weekly reports (Excel with multiple sheets)
        weekly_file = Path("inputs/weekly_reports") / f"weekly_{date_str}.xlsx"
        if not weekly_file.exists():
            with pd.ExcelWriter(weekly_file, engine='openpyxl') as writer:
                # Summary sheet
                summary_data = pd.DataFrame({
                    "WeekStart": [dt.strftime("%Y-%m-%d")],
                    "TotalSales": [round(np.random.uniform(1000, 2000), 2)],
                    "TotalOrders": [int(np.random.randint(20, 50))]
                })
                summary_data.to_excel(writer, sheet_name='Summary', index=False)
                
                # Departments sheet
                departments_data = pd.DataFrame({
                    "Department": ["Sales", "Marketing", "Operations", "Finance", "HR"],
                    "WeeklyTotal": np.round(np.random.uniform(100, 1000, 5), 2)
                })
                departments_data.to_excel(writer, sheet_name='Departments', index=False)
            print(f"Created: {weekly_file}")
    
    print("\nSample data creation completed!")

if __name__ == "__main__":
    create_sample_data()
