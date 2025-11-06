




#!/usr/bin/env python3
"""
MULTISET INSIGHTS SYSTEM - INTERACTIVE BUSINESS INTELLIGENCE
============================================================
User-driven analysis with dynamic filters and selections
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import xlsxwriter

warnings.filterwarnings('ignore')

class MultisetInsights:
    """Interactive business intelligence analyzer"""
    
    def __init__(self):
        """Initialize the insights analyzer"""
        self.datasets = {}
        self.exits_data = {}
        self.inputs_data = {}
        self.session_id = None
        self.output_dir = Path("analysis_results")
        self.output_dir.mkdir(exist_ok=True)
        self.charts_dir = Path("analysis_results/insights_charts")
        self.charts_dir.mkdir(exist_ok=True)
        
        # Column mappings
        self.exits_columns = {
            'origin_country': 'A',
            'operator': 'D',
            'agency_raw': 'F',
            'the_uniques': 'G',
            'users': 'I',
            'destination': 'J',
            'date': 'M',
            'hour': 'N',
            'amount': 'O',
            'fee': 'P'
        }
        
        self.inputs_columns = {
            'origin_country': 'Alpha',
            'operator': 'Delta',
            'agency_raw': 'Echo',
            'the_uniques': 'Foxtrot',
            'users': 'Hotel',
            'date': 'Lima',
            'hour': 'Mike',
            'intermediate': 'November',
            'amount': 'Uniform',
            'fee_raw': 'Oscar'
        }
        
    def load_datasets(self, session_id=None):
        """Load datasets from saved session"""
        storage_dir = Path("parsed_datasets")
        
        if session_id is None:
            sessions = sorted([d for d in storage_dir.iterdir() if d.is_dir()])
            if not sessions:
                print("[ERROR] No saved datasets found")
                return False
            session_dir = sessions[-1]
            self.session_id = session_dir.name.replace("session_", "")
        else:
            session_dir = storage_dir / f"session_{session_id}"
            self.session_id = session_id
        
        if not session_dir.exists():
            print(f"[ERROR] Session {session_id} not found")
            return False
        
        import pickle
        pickle_file = session_dir / "datasets.pkl"
        if pickle_file.exists():
            with open(pickle_file, 'rb') as f:
                self.datasets = pickle.load(f)
        
        for name, df in self.datasets.items():
            if name.startswith('Exits'):
                self.exits_data[name] = df
            elif name.startswith('Inputs'):
                self.inputs_data[name] = df
        
        print(f"[OK] Loaded {len(self.datasets)} datasets")
        print(f"   • Exits: {len(self.exits_data)} files")
        print(f"   • Inputs: {len(self.inputs_data)} files")
        
        return True
    
    def prepare_exits_data(self):
        """Prepare and standardize Exits dataset"""
        if not self.exits_data:
            return None
        
        all_exits = pd.concat(self.exits_data.values(), ignore_index=True)
        
        # Create standardized dataframe
        df = pd.DataFrame()
        df['origin_country'] = all_exits[self.exits_columns['origin_country']]
        df['operator'] = all_exits[self.exits_columns['operator']]
        
        # Extract agency code (last 9 chars)
        df['agency'] = all_exits[self.exits_columns['agency_raw']].apply(
            lambda x: str(x)[-9:] if pd.notna(x) and len(str(x)) >= 9 else None
        )
        
        df['the_uniques'] = all_exits[self.exits_columns['the_uniques']]
        df['users'] = all_exits[self.exits_columns['users']]
        df['destination'] = all_exits[self.exits_columns['destination']]
        
        # Parse date and hour
        df['date'] = pd.to_datetime(all_exits[self.exits_columns['date']], errors='coerce')
        df['hour'] = pd.to_numeric(all_exits[self.exits_columns['hour']].astype(str).str.split(':').str[0], errors='coerce')
        
        # Numeric fields
        df['amount'] = pd.to_numeric(all_exits[self.exits_columns['amount']], errors='coerce')
        df['fee'] = pd.to_numeric(all_exits[self.exits_columns['fee']], errors='coerce')
        
        # Add time period
        df['year_month'] = df['date'].dt.to_period('M').astype(str)
        df['hour_period'] = df['hour'].apply(self._categorize_hour)
        
        # Clean
        df = df.dropna(subset=['operator'])
        
        return df
    
    def prepare_inputs_data(self):
        """Prepare and standardize Inputs dataset with normalized fee"""
        if not self.inputs_data:
            return None
        
        all_inputs = pd.concat(self.inputs_data.values(), ignore_index=True)
        
        # Create standardized dataframe
        df = pd.DataFrame()
        df['origin_country'] = all_inputs[self.inputs_columns['origin_country']]
        df['operator'] = all_inputs[self.inputs_columns['operator']]
        
        # Extract agency code (last 9 chars)
        df['agency'] = all_inputs[self.inputs_columns['agency_raw']].apply(
            lambda x: str(x)[-9:] if pd.notna(x) and len(str(x)) >= 9 else None
        )
        
        df['the_uniques'] = all_inputs[self.inputs_columns['the_uniques']]
        df['users'] = all_inputs[self.inputs_columns['users']]
        df['destination'] = df['origin_country']  # For combined analysis
        
        # Parse date and hour
        df['date'] = pd.to_datetime(all_inputs[self.inputs_columns['date']], errors='coerce')
        df['hour'] = pd.to_numeric(all_inputs[self.inputs_columns['hour']].astype(str).str.split(':').str[0], errors='coerce')
        
        # Numeric fields
        df['amount'] = pd.to_numeric(all_inputs[self.inputs_columns['amount']], errors='coerce')
        intermediate = pd.to_numeric(all_inputs[self.inputs_columns['intermediate']], errors='coerce')
        fee_raw = pd.to_numeric(all_inputs[self.inputs_columns['fee_raw']], errors='coerce')
        
        # Calculate normalized fee: Osc_sp = (Uniform / November) * Oscar
        df['fee'] = (df['amount'] / intermediate) * fee_raw
        df['fee'] = df['fee'].replace([np.inf, -np.inf], np.nan)
        
        # Add time period
        df['year_month'] = df['date'].dt.to_period('M').astype(str)
        df['hour_period'] = df['hour'].apply(self._categorize_hour)
        
        # Clean
        df = df.dropna(subset=['operator'])
        
        return df
    
    def _categorize_hour(self, hour):
        """Categorize hour into periods"""
        if pd.isna(hour):
            return 'Unknown'
        hour = int(hour)
        if hour < 12:
            return 'Morning (0-11h)'
        elif 12 <= hour < 15:
            return 'Noon (12-14h)'
        elif 15 <= hour < 18:
            return 'Afternoon (15-17h)'
        else:
            return 'Evening (18-23h)'
    
    def analyze_dynamic(self, dataset_type, group_by, measure_by, filters=None):
        """
        Dynamic analysis based on user selections
        
        Parameters:
        - dataset_type: 'exits', 'inputs', or 'combined'
        - group_by: 'operator', 'agency', 'destination', 'users', 'origin_country'
        - measure_by: 'count', 'amount', 'fee', 'destinations', 'hours'
        - filters: dict with 'date_from', 'date_to', 'hour_period', 'destination', etc.
        """
        
        # Load data
        if dataset_type == 'exits':
            df = self.prepare_exits_data()
            if df is None:
                return None
        elif dataset_type == 'inputs':
            df = self.prepare_inputs_data()
            if df is None:
                return None
        elif dataset_type == 'combined':
            exits_df = self.prepare_exits_data()
            inputs_df = self.prepare_inputs_data()
            if exits_df is None or inputs_df is None:
                return None
            df = pd.concat([exits_df, inputs_df], ignore_index=True)
        else:
            return None
        
        # Apply filters
        if filters:
            if 'date_from' in filters and filters['date_from']:
                df = df[df['date'] >= pd.to_datetime(filters['date_from'])]
            if 'date_to' in filters and filters['date_to']:
                df = df[df['date'] <= pd.to_datetime(filters['date_to'])]
            if 'hour_period' in filters and filters['hour_period']:
                df = df[df['hour_period'] == filters['hour_period']]
            if 'destination' in filters and filters['destination']:
                df = df[df['destination'].isin(filters['destination'])]
            if 'year_month' in filters and filters['year_month']:
                df = df[df['year_month'].isin(filters['year_month'])]
        
        # Perform analysis
        if measure_by == 'count':
            result = df.groupby(group_by).size().reset_index(name='count')
            result = result.sort_values('count', ascending=False)
            
        elif measure_by == 'amount':
            result = df.groupby(group_by)['amount'].sum().reset_index()
            result.columns = [group_by, 'total_amount']
            result = result.sort_values('total_amount', ascending=False)
            
        elif measure_by == 'fee':
            result = df.groupby(group_by)['fee'].sum().reset_index()
            result.columns = [group_by, 'total_fee']
            result = result.sort_values('total_fee', ascending=False)
            
        elif measure_by == 'destinations':
            # Count unique destinations per group
            result = df.groupby(group_by)['destination'].nunique().reset_index()
            result.columns = [group_by, 'unique_destinations']
            result = result.sort_values('unique_destinations', ascending=False)
            
        elif measure_by == 'hours':
            # Analyze hour distribution per group
            result = df.groupby([group_by, 'hour']).size().reset_index(name='count')
            
        return result
    
    def analyze_cross_dimension(self, dataset_type, group_by, measure_by, cross_by, filters=None):
        """
        Cross-dimensional analysis (e.g., operator by destination by amount)
        """
        
        # Load data
        if dataset_type == 'exits':
            df = self.prepare_exits_data()
        elif dataset_type == 'inputs':
            df = self.prepare_inputs_data()
        elif dataset_type == 'combined':
            exits_df = self.prepare_exits_data()
            inputs_df = self.prepare_inputs_data()
            df = pd.concat([exits_df, inputs_df], ignore_index=True)
        else:
            return None
        
        if df is None:
            return None
        
        # Apply filters
        if filters:
            if 'date_from' in filters and filters['date_from']:
                df = df[df['date'] >= pd.to_datetime(filters['date_from'])]
            if 'date_to' in filters and filters['date_to']:
                df = df[df['date'] <= pd.to_datetime(filters['date_to'])]
            if 'hour_period' in filters and filters['hour_period']:
                df = df[df['hour_period'] == filters['hour_period']]
        
        # Cross-dimensional grouping
        if measure_by == 'amount':
            result = df.groupby([group_by, cross_by])['amount'].sum().reset_index()
            result.columns = [group_by, cross_by, 'total_amount']
        elif measure_by == 'fee':
            result = df.groupby([group_by, cross_by])['fee'].sum().reset_index()
            result.columns = [group_by, cross_by, 'total_fee']
        elif measure_by == 'count':
            result = df.groupby([group_by, cross_by]).size().reset_index(name='count')
        
        return result
    
    def create_slider_chart(self, data, x_col, y_col, title, slider_values=[15, 50, 100, 200, 500]):
        """Create interactive chart with slider"""
        
        max_val = len(data)
        slider_values = [v for v in slider_values if v <= max_val]
        if not slider_values:
            slider_values = [min(15, max_val)]
        
        fig = go.Figure()
        colors = (px.colors.qualitative.Plotly + px.colors.qualitative.Set2) * 20
        
        for top_n in slider_values:
            data_slice = data.head(top_n)
            visible = (top_n == slider_values[0])
            
            fig.add_trace(go.Bar(
                x=data_slice[x_col],
                y=data_slice[y_col],
                marker_color=colors[:len(data_slice)],
                text=data_slice[y_col].apply(lambda x: f'{x:,.0f}'),
                textposition='auto',
                visible=visible,
                hovertemplate=f'<b>%{{x}}</b><br>{y_col}: %{{y:,.2f}}<extra></extra>'
            ))
        
        steps = []
        for i, top_n in enumerate(slider_values):
            step = dict(
                method="update",
                args=[{"visible": [False] * len(slider_values)},
                      {"title": f"{title} (Top {top_n})"}],
                label=f"{top_n}"
            )
            step["args"][0]["visible"][i] = True
            steps.append(step)
        
        sliders = [dict(
            active=0,
            yanchor="top",
            y=-0.2,
            xanchor="left",
            x=0.1,
            currentvalue=dict(prefix="Showing Top: ", visible=True),
            pad=dict(b=10, t=50),
            len=0.8,
            steps=steps
        )]
        
        fig.update_layout(
            title=f"{title} (Top {slider_values[0]})",
            xaxis_title=x_col,
            yaxis_title=y_col,
            sliders=sliders,
            height=700,
            template='plotly_white',
            showlegend=False,
            xaxis_tickangle=-45
        )
        
        return fig
    
    def create_heatmap(self, data, group_col, cross_col, value_col, title):
        """Create heatmap for cross-dimensional analysis"""
        
        # Pivot data
        pivot = data.pivot_table(
            index=group_col,
            columns=cross_col,
            values=value_col,
            aggfunc='sum',
            fill_value=0
        )
        
        # Limit to top groups
        top_groups = pivot.sum(axis=1).nlargest(20).index
        pivot = pivot.loc[top_groups]
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index,
            colorscale='Viridis',
            text=pivot.values,
            texttemplate='%{text:,.0f}',
            textfont={"size": 8},
            hovertemplate=f'{group_col}: %{{y}}<br>{cross_col}: %{{x}}<br>{value_col}: %{{z:,.2f}}<extra></extra>'
        ))
        
        fig.update_layout(
            title=title,
            xaxis_title=cross_col,
            yaxis_title=group_col,
            height=700,
            template='plotly_white'
        )
        
        return fig
    
    def run_example_analyses(self):
        """Run example analyses for demonstration"""
        print("\n" + "="*70)
        print("[EXECUTE] RUNNING EXAMPLE ANALYSES")
        print("="*70)
        
        results = {}
        
        # Example 1: Operator by Fee (Exits)
        print("\n[1] Operator by Total Fee (Exits)")
        result = self.analyze_dynamic('exits', 'operator', 'fee')
        if result is not None:
            fig = self.create_slider_chart(result, 'operator', 'total_fee', 'Operators by Total Fee (Exits)')
            fig.write_html(str(self.charts_dir / 'operator_fee_exits.html'))
            results['operator_fee_exits'] = result
            print(f"   ✓ Found {len(result)} operators")
        
        # Example 2: Operator by Destination Count (Exits)
        print("\n[2] Operator by Destination Count (Exits)")
        result = self.analyze_dynamic('exits', 'operator', 'destinations')
        if result is not None:
            fig = self.create_slider_chart(result, 'operator', 'unique_destinations', 'Operators by Destination Count')
            fig.write_html(str(self.charts_dir / 'operator_destinations.html'))
            results['operator_destinations'] = result
            print(f"   ✓ Analyzed {len(result)} operators")
        
        # Example 3: Agency by Amount (Exits)
        print("\n[3] Agency by Total Amount (Exits)")
        result = self.analyze_dynamic('exits', 'agency', 'amount')
        if result is not None:
            result = result[result['agency'].notna()]
            fig = self.create_slider_chart(result, 'agency', 'total_amount', 'Agencies by Total Amount')
            fig.write_html(str(self.charts_dir / 'agency_amount.html'))
            results['agency_amount'] = result
            print(f"   ✓ Found {len(result)} agencies")
        
        # Example 4: Combined Fee Analysis
        print("\n[4] Combined Operator Fee (Exits + Inputs)")
        result = self.analyze_dynamic('combined', 'operator', 'fee')
        if result is not None:
            fig = self.create_slider_chart(result, 'operator', 'total_fee', 'Operators by Total Fee (Combined)')
            fig.write_html(str(self.charts_dir / 'operator_fee_combined.html'))
            results['operator_fee_combined'] = result
            print(f"   ✓ Combined analysis: {len(result)} operators")
        
        # Example 5: Operator x Destination (Cross-dimensional)
        print("\n[5] Operator by Destination Amount (Cross-dimensional)")
        result = self.analyze_cross_dimension('exits', 'operator', 'amount', 'destination')
        if result is not None:
            fig = self.create_heatmap(result, 'operator', 'destination', 'total_amount', 
                                     'Operator × Destination Heatmap')
            fig.write_html(str(self.charts_dir / 'operator_destination_heatmap.html'))
            results['operator_destination'] = result
            print(f"   ✓ Cross-analysis: {len(result)} combinations")
        
        # Save to Excel
        print("\n[6] Saving to Excel...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_file = self.output_dir / f"insights_example_{timestamp}.xlsx"
        
        with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
            for key, df in results.items():
                sheet_name = key.replace('_', ' ').title()[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"   ✓ Saved to: {excel_file}")
        
        print("\n" + "="*70)
        print("[OK] EXAMPLE ANALYSES COMPLETE")
        print("="*70)
        
        return str(excel_file), {}


def main():
    """Main function"""
    print("\n" + "="*70)
    print("MULTISET INSIGHTS - INTERACTIVE BUSINESS INTELLIGENCE")
    print("="*70)
    
    insights = MultisetInsights()
    
    print("\n[LOAD] Loading datasets...")
    if not insights.load_datasets():
        print("[ERROR] Failed to load datasets")
        return None, None
    
    print("\n[NOTE] Running example analyses...")
    print("       In production, user will select via web interface")
    
    excel_file, chart_configs = insights.run_example_analyses()
    
    print("\n[OK] Insights ready for interactive use!")
    return excel_file, chart_configs


if __name__ == "__main__":
    main()