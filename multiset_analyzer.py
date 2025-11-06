




#!/usr/bin/env python3
"""
MULTISET ANALYZER SYSTEM - REFACTORED
=====================================
Comprehensive analysis tool with single Excel output and interactive visualizations
"""

import pandas as pd
import numpy as np
import os
import sys
import pickle
import json
from pathlib import Path
from datetime import datetime
import warnings
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import xlsxwriter
from io import BytesIO
import base64
from collections import defaultdict, Counter

warnings.filterwarnings('ignore')

class MultisetAnalyzer:
    """Main analyzer class for comprehensive dataset analysis"""
    
    def __init__(self):
        """Initialize the analyzer"""
        self.datasets = {}
        self.exits_data = {}
        self.inputs_data = {}
        self.waves_data = {}
        self.session_id = None
        self.output_dir = Path("analysis_results")
        self.output_dir.mkdir(exist_ok=True)
        self.charts_dir = Path("analysis_results/charts")
        self.charts_dir.mkdir(exist_ok=True)
        self.analysis_results = {}
        self.chart_configs = {}
        
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
        
        # Load datasets
        pickle_file = session_dir / "datasets.pkl"
        if pickle_file.exists():
            with open(pickle_file, 'rb') as f:
                self.datasets = pickle.load(f)
        
        # Organize datasets by type
        for name, df in self.datasets.items():
            if name.startswith('Exits'):
                self.exits_data[name] = df
            elif name.startswith('Inputs'):
                self.inputs_data[name] = df
            elif name.startswith('Waves'):
                self.waves_data[name] = df
        
        print(f"[OK] Loaded {len(self.datasets)} datasets from session {self.session_id}")
        print(f"   • Exits: {len(self.exits_data)} files")
        print(f"   • Inputs: {len(self.inputs_data)} files")
        print(f"   • Waves: {len(self.waves_data)} files")
        
        return True
    
    def create_interactive_chart(self, data, x_col, y_col, title, chart_id, top_n=20):
        """Create an interactive Plotly chart with different colors for each bar"""
        # Limit data to top_n
        data_slice = data.head(top_n).copy()
        
        # Generate unique colors for each bar
        colors = px.colors.qualitative.Plotly * 3  # Repeat colors if needed
        bar_colors = colors[:len(data_slice)]
        
        # Create bar chart
        fig = go.Figure(data=[
            go.Bar(
                x=data_slice[x_col],
                y=data_slice[y_col],
                marker_color=bar_colors,
                text=data_slice[y_col].apply(lambda x: f'{x:,.0f}' if x > 100 else f'{x:,.2f}'),
                textposition='auto',
            )
        ])
        
        # Update layout
        fig.update_layout(
            title=title + f' (Top {top_n})',
            xaxis_title=x_col,
            yaxis_title=y_col,
            showlegend=False,
            height=500,
            xaxis_tickangle=-45,
            template='plotly_white'
        )
        
        # Save as HTML
        html_path = self.charts_dir / f"{chart_id}_top{top_n}.html"
        fig.write_html(str(html_path))
        
        # Save as static image
        try:
            img_path = self.charts_dir / f"{chart_id}_top{top_n}.png"
            fig.write_image(str(img_path), width=800, height=500)
            return str(img_path)
        except:
            print(f"[WARNING] Could not save static image for {chart_id}")
            return None
    
    def analyze_unique_destinations(self):
        """Analysis 1: Analyze unique destinations and their total amounts"""
        print("\n[ANALYSIS 1] UNIQUE DESTINATION ANALYSIS")
        
        if not self.exits_data:
            print("[ERROR] No Exits data available")
            return None
        
        # Merge all exits data
        all_exits = pd.concat(self.exits_data.values(), ignore_index=True)
        
        # Extract destinations and amounts
        destination_data = []
        for idx, row in all_exits.iterrows():
            try:
                dest = str(row['J']).strip() if pd.notna(row['J']) else None
                amount = pd.to_numeric(row['O'], errors='coerce') if pd.notna(row['O']) else 0
                if dest and dest != '' and amount > 0:
                    destination_data.append({'destination': dest, 'amount': amount})
            except:
                continue
        
        df_dest = pd.DataFrame(destination_data)
        
        if df_dest.empty:
            print("[ERROR] No valid destination data found")
            return None
        
        # Group by destination
        dest_summary = df_dest.groupby('destination').agg(
            count=('amount', 'count'),
            total=('amount', 'sum')
        ).reset_index()
        
        # Sort for different views
        dest_by_count = dest_summary.sort_values('count', ascending=False).reset_index(drop=True)
        dest_by_amount = dest_summary.sort_values('total', ascending=False).reset_index(drop=True)
        
        # Store results
        self.analysis_results['dest_by_count'] = dest_by_count
        self.analysis_results['dest_by_amount'] = dest_by_amount
        
        # Create charts for different top_n values
        for top_n in [5, 10, 20, 30]:
            self.create_interactive_chart(
                dest_by_count, 'destination', 'count',
                'Destinations by Transaction Count', 
                'dest_count', top_n
            )
            self.create_interactive_chart(
                dest_by_amount, 'destination', 'total',
                'Destinations by Total Amount',
                'dest_amount', top_n
            )
        
        # Store chart configs for UI
        self.chart_configs['dest_count'] = {
            'title': 'Destinations by Transaction Count',
            'data': dest_by_count.to_dict('records')
        }
        self.chart_configs['dest_amount'] = {
            'title': 'Destinations by Total Amount',
            'data': dest_by_amount.to_dict('records')
        }
        
        print(f"[OK] Found {len(dest_summary)} unique destinations")
        return dest_summary
    
    def analyze_mean_amounts(self):
        """Analysis 2: Calculate and visualize mean amounts per destination"""
        print("\n[ANALYSIS 2] MEAN AMOUNT PER DESTINATION")
        
        if 'dest_by_count' not in self.analysis_results:
            self.analyze_unique_destinations()
        
        if 'dest_by_count' not in self.analysis_results:
            print("[ERROR] Could not calculate mean amounts")
            return None
        
        # Calculate mean from the summary
        dest_summary = self.analysis_results['dest_by_count'].copy()
        dest_summary['mean_amount'] = dest_summary['total'] / dest_summary['count']
        dest_mean = dest_summary.sort_values('mean_amount', ascending=False).reset_index(drop=True)
        
        # Store results
        self.analysis_results['dest_mean'] = dest_mean
        
        # Create charts
        for top_n in [5, 10, 20, 30]:
            self.create_interactive_chart(
                dest_mean, 'destination', 'mean_amount',
                'Mean Amount per Destination',
                'dest_mean', top_n
            )
        
        # Store chart config
        self.chart_configs['dest_mean'] = {
            'title': 'Mean Amount per Destination',
            'data': dest_mean.to_dict('records')
        }
        
        print(f"[OK] Mean analysis complete")
        return dest_mean
    
    def analyze_user_red_flags(self):
        """Analysis 3: Find users with red flags based on Waves dataset matching"""
        print("\n[ANALYSIS 3] USER RED FLAG ANALYSIS")
        
        if not self.exits_data or not self.waves_data:
            print("[ERROR] Required data not available")
            return None
        
        # Merge datasets
        all_exits = pd.concat(self.exits_data.values(), ignore_index=True)
        all_waves = pd.concat(self.waves_data.values(), ignore_index=True)
        
        # Helper: normalize to digits-only and pad to 10 digits
        def _norm_op(val):
            if pd.isna(val):
                return None
            s = ''.join(ch for ch in str(val).strip() if ch.isdigit())
            if not s:
                return None
            return s.zfill(10) if len(s) <= 10 else s
        
        # Create normalized columns
        if 'S1' not in all_waves.columns:
            print("[ERROR] Column 'S1' not found in Waves")
            return None
        all_waves['S1_norm'] = all_waves['S1'].apply(_norm_op)
        
        if 'G' not in all_exits.columns:
            print("[ERROR] Column 'G' not found in Exits")
            return None
        all_exits['G_norm'] = all_exits['G'].apply(_norm_op)
        
        # Build set of normalized operations from Waves
        waves_operations = set(all_waves['S1_norm'].dropna().astype(str))
        print(f"[OK] Found {len(waves_operations)} unique operations in Waves")
        
        # Analyze users
        user_analysis = []
        for _, row in all_exits.iterrows():
            try:
                user = (str(row['I']).strip() if pd.notna(row['I']) else None) if 'I' in all_exits.columns else None
                amount = pd.to_numeric(row['O'], errors='coerce') if ('O' in all_exits.columns and pd.notna(row.get('O'))) else 0
                operation = row.get('G_norm')
                
                if user and user != '':
                    red_flag = (operation in waves_operations) if operation else False
                    user_analysis.append({
                        'user': user,
                        'amount': float(amount) if pd.notna(amount) else 0.0,
                        'operation': operation,
                        'red_flag': bool(red_flag)
                    })
            except Exception:
                continue
        
        df_users = pd.DataFrame(user_analysis)
        if df_users.empty:
            print("[ERROR] No valid user data found")
            return None
        
        # Aggregate by user
        user_summary = df_users.groupby('user').agg(
            total_amount=('amount', 'sum'),
            transaction_count=('amount', 'count'),
            red_flag_count=('red_flag', 'sum')
        ).reset_index()
        user_summary['has_red_flag'] = user_summary['red_flag_count'] > 0
        user_summary = user_summary.sort_values('total_amount', ascending=False).reset_index(drop=True)
        
        # Store results
        self.analysis_results['red_flags'] = user_summary
        
        print(f"[OK] Total users: {len(user_summary)}")
        print(f"[OK] Red-flagged users: {user_summary['has_red_flag'].sum()}")
        
        return user_summary
    
    def analyze_user_details(self):
        """Analysis 4: Show user details with Red_Flag column and per-user row coloring (handled in save_to_excel)"""
        print("\n[ANALYSIS 4] USER DETAILS WITH RED FLAGS")

        if not self.exits_data:
            print("[ERROR] No Exits data available")
            return None

        # Merge all exits data
        all_exits = pd.concat(self.exits_data.values(), ignore_index=True)

        # --- Build red-flag operation set using same normalization as analyze_user_red_flags ---
        waves_operations = set()
        if self.waves_data:
            all_waves = pd.concat(self.waves_data.values(), ignore_index=True)

            def _norm_op(val):
                if pd.isna(val):
                    return None
                s = ''.join(ch for ch in str(val).strip() if ch.isdigit())
                if not s:
                    return None
                return s.zfill(10) if len(s) <= 10 else s

            if 'S1' in all_waves.columns:
                all_waves['S1_norm'] = all_waves['S1'].apply(_norm_op)
                waves_operations = set(all_waves['S1_norm'].dropna().astype(str))
            else:
                print("[ERROR] Column 'S1' not found in Waves; Red_Flag column will be empty")

            if 'G' in all_exits.columns:
                all_exits['G_norm'] = all_exits['G'].apply(_norm_op)
            else:
                print("[ERROR] Column 'G' not found in Exits; Red_Flag column will be empty")
                all_exits['G_norm'] = None

        # --- Build detail rows with Red_Flag ---
        user_details = []
        for _, row in all_exits.iterrows():
            try:
                user = str(row.get('I')).strip() if pd.notna(row.get('I')) else None
                withdrawer = str(row.get('H')).strip() if pd.notna(row.get('H')) else None
                date = str(row.get('M')).strip() if pd.notna(row.get('M')) else None
                destination = str(row.get('J')).strip() if pd.notna(row.get('J')) else None
                amount = pd.to_numeric(row.get('O'), errors='coerce') if pd.notna(row.get('O')) else 0
                op = str(row.get('G')).strip() if pd.notna(row.get('G')) else None
                op_norm = str(row.get('G_norm')).strip() if pd.notna(row.get('G_norm')) else None

                if user and amount and amount > 0:
                    red_flag = 'Yes' if (op_norm and op_norm in waves_operations) else ''
                    user_details.append({
                        'User': user,
                        'Withdrawer': withdrawer,
                        'Date': date,
                        'Destination': destination,
                        'Amount': float(amount) if pd.notna(amount) else 0.0,
                        'Operation': op,
                        'Red_Flag': red_flag
                    })
            except Exception:
                continue

        df_details = pd.DataFrame(user_details)
        if df_details.empty:
            print("[ERROR] No valid user details found")
            return None

        # Totals per user
        user_totals = df_details.groupby('User')['Amount'].sum().reset_index()
        user_totals.columns = ['User', 'Total_Amount']
        df_final = df_details.merge(user_totals, on='User', how='left')

        # Final column order: Red_Flag LAST
        desired_cols = ['User', 'Withdrawer', 'Date', 'Destination',
                        'Amount', 'Operation', 'Total_Amount', 'Red_Flag']
        df_final = df_final[[c for c in desired_cols if c in df_final.columns]]

        # Safe sort (Date may be text)
        sort_cols = [c for c in ['Total_Amount', 'User', 'Date'] if c in df_final.columns]
        df_final = df_final.sort_values(sort_cols, ascending=[False, True, True][:len(sort_cols)]).reset_index(drop=True)

        self.analysis_results['user_details'] = df_final
        print(f"[OK] Total transactions: {len(df_final)}")
        print(f"[OK] Unique users: {df_final['User'].nunique()}")

        return df_final
    
    def analyze_operations(self):
        """Analysis 5: Cross-reference operations between Waves, Exits, and Inputs"""
        print("\n[ANALYSIS 5] OPERATION NUMBER ANALYSIS")
        
        if not all([self.exits_data, self.inputs_data, self.waves_data]):
            print("[ERROR] All three dataset types required")
            return None
        
        # Merge datasets
        all_exits = pd.concat(self.exits_data.values(), ignore_index=True)
        all_inputs = pd.concat(self.inputs_data.values(), ignore_index=True)
        all_waves = pd.concat(self.waves_data.values(), ignore_index=True)
        
        # Column guards
        if 'S1' not in all_waves.columns:
            print("[ERROR] Column 'S1' not found in Waves")
            return None
        
        # Helper: normalize to 10-digit, digits-only string
        def _norm10(val):
            if pd.isna(val):
                return None
            s = ''.join(ch for ch in str(val).strip() if ch.isdigit())
            if not s:
                return None
            if len(s) < 10:
                s = s.zfill(10)
            elif len(s) > 10:
                s = s[-10:]
            return s
        
        # Create normalized columns
        all_waves['S1_norm'] = all_waves['S1'].apply(_norm10)
        all_exits['G_norm'] = all_exits['G'].apply(_norm10) if 'G' in all_exits.columns else None
        all_inputs['Foxtrot_norm'] = all_inputs['Foxtrot'].apply(_norm10) if 'Foxtrot' in all_inputs.columns else None
        
        # Unique normalized operations from Waves
        waves_operations = all_waves['S1_norm'].dropna().unique()
        
        operation_analysis = []
        
        for operation in waves_operations:
            if not operation:
                continue
            
            # Match on normalized columns
            exits_matches = all_exits[all_exits['G_norm'] == operation] if 'G_norm' in all_exits.columns else pd.DataFrame()
            inputs_matches = all_inputs[all_inputs['Foxtrot_norm'] == operation] if 'Foxtrot_norm' in all_inputs.columns else pd.DataFrame()
            
            if not exits_matches.empty and not inputs_matches.empty:
                # Found in both - A/R
                destination = "A/R"
                exits_amount = pd.to_numeric(exits_matches['O'], errors='coerce').sum() if 'O' in exits_matches.columns else 0
                inputs_amount = pd.to_numeric(inputs_matches['November'], errors='coerce').sum() if 'November' in inputs_matches.columns else 0
                total_amount = (exits_amount if pd.notna(exits_amount) else 0) + \
                              (inputs_amount if pd.notna(inputs_amount) else 0)
                
                operation_analysis.append({
                    'Operation': operation,
                    'Destination': destination,
                    'Amount': total_amount,
                    'Source': 'Both'
                })
            
            elif not exits_matches.empty:
                # Found only in Exits
                for _, row in exits_matches.iterrows():
                    dest = str(row['J']).strip() if ('J' in exits_matches.columns and pd.notna(row.get('J'))) else "Unknown"
                    amount = pd.to_numeric(row['O'], errors='coerce') if 'O' in exits_matches.columns else 0
                    operation_analysis.append({
                        'Operation': operation,
                        'Destination': dest,
                        'Amount': float(amount) if pd.notna(amount) else 0.0,
                        'Source': 'Sends'
                    })
            
            elif not inputs_matches.empty:
                # Found only in Inputs - withdrawal
                destination = "Withdrawal"
                total_amount = pd.to_numeric(inputs_matches['November'], errors='coerce').sum() if 'November' in inputs_matches.columns else 0
                operation_analysis.append({
                    'Operation': operation,
                    'Destination': destination,
                    'Amount': float(total_amount) if pd.notna(total_amount) else 0.0,
                    'Source': 'Receives'
                })
            
            else:
                # Not found in either
                operation_analysis.append({
                    'Operation': operation,
                    'Destination': "Not Found",
                    'Amount': 0.0,
                    'Source': 'None'
                })
        
        df_operations = pd.DataFrame(operation_analysis)
        
        if df_operations.empty:
            print("[OK] No operation data found")
            return None
        
        # Store results
        self.analysis_results['operations'] = df_operations
        
        print(f"[OK] Operations analyzed: {len(waves_operations)}")
        print(f"[OK] Operations with data: {len(df_operations)}")
        
        return df_operations
    
    def analyze_one_to_many(self):
        """Analysis 6: Find users who send to multiple different receivers"""
        print("\n[ANALYSIS 6] ONE-TO-MANY ANALYSIS")
        
        if not self.exits_data:
            print("[ERROR] No Exits data available")
            return None
        
        # Merge all exits data
        all_exits = pd.concat(self.exits_data.values(), ignore_index=True)
        
        # Find users with multiple receivers
        user_receivers = defaultdict(set)
        user_transactions = defaultdict(list)
        user_amounts = defaultdict(list)
        
        for idx, row in all_exits.iterrows():
            try:
                user = str(row['I']).strip() if pd.notna(row['I']) else None
                receiver = str(row['H']).strip() if pd.notna(row['H']) else None
                
                if user and receiver and user != '' and receiver != '':
                    user_receivers[user].add(receiver)
                    
                    amount = pd.to_numeric(row['O'], errors='coerce') if pd.notna(row['O']) else 0
                    user_amounts[user].append(amount)
                    
                    transaction = {
                        'Operation': str(row['G']).strip() if pd.notna(row['G']) else '',
                        'Withdrawer': receiver,
                        'User': user,
                        'Destination': str(row['J']).strip() if pd.notna(row['J']) else '',
                        'Date': str(row['M']).strip() if pd.notna(row['M']) else '',
                        'Reference': str(row['N']).strip() if pd.notna(row['N']) else '',
                        'Amount': amount
                    }
                    user_transactions[user].append(transaction)
            except:
                continue
        
        # Filter users with more than 2 receivers
        multi_receiver_users = {user: receivers for user, receivers in user_receivers.items() 
                               if len(receivers) > 2}
        
        if not multi_receiver_users:
            print("[OK] No users found with more than 2 receivers")
            return None
        
        # Create detailed report
        all_transactions = []
        for user in multi_receiver_users:
            transactions = user_transactions[user]
            for trans in transactions:
                trans['Unique_Receivers'] = len(multi_receiver_users[user])
                all_transactions.append(trans)
        
        df_one_to_many = pd.DataFrame(all_transactions)
        df_one_to_many = df_one_to_many.sort_values(['Unique_Receivers', 'User', 'Date'], 
                                                   ascending=[False, True, True])
        
        # Create summary report
        summary_rows = []
        for user in sorted(multi_receiver_users.keys()):
            summary_rows.append({
                'User': user,
                'Uniq_Rcvrs': len(multi_receiver_users[user]),
                'Total_Trx': len(user_transactions[user]),
                'Total_Amt': sum(user_amounts[user]),
                'Avg_Amt': sum(user_amounts[user]) / len(user_amounts[user]) if user_amounts[user] else 0
            })
        
        df_otm_summary = pd.DataFrame(summary_rows)
        df_otm_summary = df_otm_summary.sort_values(['Uniq_Rcvrs', 'Total_Trx'], 
                                                     ascending=[False, False]).reset_index(drop=True)
        
        # Store results
        self.analysis_results['one_to_many'] = df_one_to_many
        self.analysis_results['OtM-Summary'] = df_otm_summary
        
        print(f"[OK] Users with >2 receivers: {len(multi_receiver_users)}")
        print(f"[OK] Total transactions: {len(df_one_to_many)}")
        print(f"[OK] Summary created with {len(df_otm_summary)} users")
        
        return df_one_to_many

    def analyze_many_to_one(self):
        """Analysis 7: Find receivers who receive from multiple different users"""
        print("\n[ANALYSIS 7] MANY-TO-ONE ANALYSIS")
        
        if not self.inputs_data:
            print("[ERROR] No Inputs data available")
            return None
        
        # Merge all inputs data
        all_inputs = pd.concat(self.inputs_data.values(), ignore_index=True)
        
        # Find receivers with multiple senders
        receiver_senders = defaultdict(set)
        receiver_transactions = defaultdict(list)
        receiver_amounts = defaultdict(list)
        
        for idx, row in all_inputs.iterrows():
            try:
                receiver = str(row['Golf']).strip() if pd.notna(row['Golf']) else None
                sender = str(row['Hotel']).strip() if pd.notna(row['Hotel']) else None
                
                if receiver and sender and receiver != '' and sender != '':
                    receiver_senders[receiver].add(sender)
                    
                    amount = pd.to_numeric(row['November'], errors='coerce') if pd.notna(row['November']) else 0
                    receiver_amounts[receiver].append(amount)
                    
                    transaction = {
                        'Receiver': receiver,
                        'Sender': sender,
                        'Date': str(row['Lima']).strip() if pd.notna(row['Lima']) else '',
                        'Amount': amount,
                        'Operation': str(row['Foxtrot']).strip() if pd.notna(row['Foxtrot']) else ''
                    }
                    receiver_transactions[receiver].append(transaction)
            except:
                continue
        
        # Filter receivers with more than 2 senders
        multi_sender_receivers = {receiver: senders for receiver, senders in receiver_senders.items() 
                                 if len(senders) > 2}
        
        if not multi_sender_receivers:
            print("[OK] No receivers found with more than 2 senders")
            return None
        
        # Create detailed report
        all_transactions = []
        for receiver in multi_sender_receivers:
            transactions = receiver_transactions[receiver]
            for trans in transactions:
                trans['Unique_Senders'] = len(multi_sender_receivers[receiver])
                all_transactions.append(trans)
        
        df_many_to_one = pd.DataFrame(all_transactions)
        df_many_to_one = df_many_to_one.sort_values(['Unique_Senders', 'Receiver', 'Date'], 
                                                   ascending=[False, True, True])
        
        # Create summary report
        summary_rows = []
        for receiver in sorted(multi_sender_receivers.keys()):
            summary_rows.append({
                'Receiver': receiver,
                'Uniq_Sndrs': len(multi_sender_receivers[receiver]),
                'Total_Trx': len(receiver_transactions[receiver]),
                'Total_Amt': sum(receiver_amounts[receiver]),
                'Avg_Amt': sum(receiver_amounts[receiver]) / len(receiver_amounts[receiver]) if receiver_amounts[receiver] else 0
            })
        
        df_mto_summary = pd.DataFrame(summary_rows)
        df_mto_summary = df_mto_summary.sort_values(['Uniq_Sndrs', 'Total_Trx'], 
                                                     ascending=[False, False]).reset_index(drop=True)
        
        # Store results
        self.analysis_results['many_to_one'] = df_many_to_one
        self.analysis_results['MtO-Summary'] = df_mto_summary
        
        print(f"[OK] Receivers with >2 senders: {len(multi_sender_receivers)}")
        print(f"[OK] Total transactions: {len(df_many_to_one)}")
        print(f"[OK] Summary created with {len(df_mto_summary)} receivers")
        
        return df_many_to_one

    def analyze_geometric_patterns(self):
        """Analysis 8: Detect circular transaction patterns (2-way and 3-way cycles) using NetworkX.
        Excludes auto-transfers (same person under name normalization)."""

        print("\n[ANALYSIS 8] GEOMETRIC PATTERN SEARCHES (via NetworkX)")

        import unicodedata
        import networkx as nx
        from difflib import SequenceMatcher

        if not self.exits_data or not self.inputs_data:
            print("[ERROR] Both Exits and Inputs data required")
            return None

        # --- Helpers: normalize names & compare persons ---

        def _strip_accents(s: str) -> str:
            return ''.join(ch for ch in unicodedata.normalize('NFKD', s) if not unicodedata.combining(ch))

        def _tokens(name: str):
            if not name:
                return []
            name = _strip_accents(str(name)).lower()
            name = ''.join(ch if ch.isalnum() or ch.isspace() else ' ' for ch in name)
            return [t for t in name.split() if len(t) >= 2]

        def canonical_name(name: str) -> str:
            toks = _tokens(name)
            if not toks:
                return ''
            if len(toks) == 1:
                return toks[0]
            return f"{toks[0]} {toks[-1]}"  # first + last token

        def same_person(a: str, b: str) -> bool:
            if not a or not b:
                return False
            if canonical_name(a) == canonical_name(b):
                return True
            ta, tb = set(_tokens(a)), set(_tokens(b))
            if len(min(ta, tb, key=len)) >= 2 and (ta.issubset(tb) or tb.issubset(ta)):
                return True
            ratio = SequenceMatcher(None, ' '.join(sorted(ta)), ' '.join(sorted(tb))).ratio()
            return ratio >= 0.9

        # --- Build a directed graph ---
        G = nx.DiGraph()

        def add_edge(sender_raw, receiver_raw, date_raw, amt_raw, source):
            if pd.isna(sender_raw) or pd.isna(receiver_raw):
                return
            s_raw = str(sender_raw).strip()
            r_raw = str(receiver_raw).strip()
            if not s_raw or not r_raw:
                return
            # exclude auto transfers
            if same_person(s_raw, r_raw):
                return
            s, r = canonical_name(s_raw), canonical_name(r_raw)
            if not s or not r or s == r:
                return
            amt = pd.to_numeric(amt_raw, errors='coerce') if amt_raw is not None else 0
            # add to graph
            if G.has_edge(s, r):
                # accumulate amounts
                G[s][r]['amounts'].append(float(amt) if pd.notna(amt) else 0.0)
            else:
                G.add_edge(s, r, amounts=[float(amt) if pd.notna(amt) else 0.0], source=source)

        # Exits dataset: I -> H
        all_exits = pd.concat(self.exits_data.values(), ignore_index=True)
        for _, row in all_exits.iterrows():
            try:
                add_edge(row.get('I'), row.get('H'), row.get('M'), row.get('O'), 'Exits')
            except Exception:
                continue

        # Inputs dataset: Hotel -> Golf
        all_inputs = pd.concat(self.inputs_data.values(), ignore_index=True)
        for _, row in all_inputs.iterrows():
            try:
                add_edge(row.get('Hotel'), row.get('Golf'), row.get('Lima'), row.get('November'), 'Inputs')
            except Exception:
                continue

        if G.number_of_edges() == 0:
            print("[OK] No valid edges after filtering")
            return None

        # --- Detect cycles (only 2- and 3-length) ---
        patterns = []
        for cycle in nx.simple_cycles(G):
            if 2 <= len(cycle) <= 3:
                # ensure cycle closes (NetworkX gives cycle list without repeating start)
                path = cycle + [cycle[0]]
                # total amount = sum of all edge amounts
                amt_sum = 0.0
                for i in range(len(path) - 1):
                    data = G.get_edge_data(path[i], path[i + 1], {})
                    amt_sum += sum(data.get('amounts', []))
                patterns.append({
                    'Type': f"{len(cycle)}-way",
                    'Pattern': " → ".join(path),
                    'Nodes': cycle,
                    'Total_Amount': amt_sum,
                    'Edge_Count': len(cycle)
                })

        if not patterns:
            print("[OK] No geometric patterns found")
            df_patterns = pd.DataFrame()
        else:
            df_patterns = pd.DataFrame(patterns).sort_values(
                ['Type', 'Total_Amount'], ascending=[True, False]
            ).reset_index(drop=True)
            print(f"[OK] Found {len(df_patterns)} cycles (length 2 or 3)")

        self.analysis_results['geometric_patterns'] = df_patterns
        return df_patterns
    

    def analyze_unique_origins(self):
        """Analysis 9: Analyze unique origins and their total amounts"""
        print("\n[ANALYSIS 9] UNIQUE ORIGIN ANALYSIS")
        
        if not self.inputs_data:
            print("[ERROR] No Inputs data available")
            return None
        
        # Merge all inputs data
        all_inputs = pd.concat(self.inputs_data.values(), ignore_index=True)
        
        # Check for required columns
        if 'Alpha' not in all_inputs.columns or 'Uniform' not in all_inputs.columns:
            print(f"[ERROR] Required columns 'Alpha' or 'Uniform' not found in Inputs data.")
            return None
            
        # Extract origins and amounts
        origin_data = []
        for idx, row in all_inputs.iterrows():
            try:
                origin = str(row['Alpha']).strip() if pd.notna(row['Alpha']) else None
                amount = pd.to_numeric(row['Uniform'], errors='coerce') if pd.notna(row['Uniform']) else 0
                if origin and origin != '' and amount > 0:
                    origin_data.append({'origin': origin, 'amount': amount})
            except:
                continue
        
        df_origin = pd.DataFrame(origin_data)
        
        if df_origin.empty:
            print("[ERROR] No valid origin data found")
            return None
        
        # Group by origin
        origin_summary = df_origin.groupby('origin').agg(
            count=('amount', 'count'),
            total=('amount', 'sum')
        ).reset_index()
        
        # Sort for different views
        origin_by_count = origin_summary.sort_values('count', ascending=False).reset_index(drop=True)
        origin_by_amount = origin_summary.sort_values('total', ascending=False).reset_index(drop=True)
        
        # Store results
        self.analysis_results['origin_by_count'] = origin_by_count
        self.analysis_results['origin_by_amount'] = origin_by_amount
        
        # Create charts for different top_n values
        for top_n in [5, 10, 20, 30]:
            self.create_interactive_chart(
                origin_by_count, 'origin', 'count',
                'Origins by Transaction Count', 
                'origin_count', top_n
            )
            self.create_interactive_chart(
                origin_by_amount, 'origin', 'total',
                'Origins by Total Amount',
                'origin_amount', top_n
            )
        
        # Store chart configs for UI
        self.chart_configs['origin_count'] = {
            'title': 'Origins by Transaction Count',
            'data': origin_by_count.to_dict('records')
        }
        self.chart_configs['origin_amount'] = {
            'title': 'Origins by Total Amount',
            'data': origin_by_amount.to_dict('records')
        }
        
        print(f"[OK] Found {len(origin_summary)} unique origins")
        return origin_summary

    def analyze_mean_origin_amounts(self):
        """Analysis 10: Calculate and visualize mean amounts per origin"""
        print("\n[ANALYSIS 10] MEAN AMOUNT PER ORIGIN")
        
        if 'origin_by_count' not in self.analysis_results:
            # Run the primary origin analysis first if not already done
            self.analyze_unique_origins()
        
        if 'origin_by_count' not in self.analysis_results:
            print("[ERROR] Could not calculate mean origin amounts (dependency failed)")
            return None
        
        # Calculate mean from the summary
        origin_summary = self.analysis_results['origin_by_count'].copy()
        origin_summary['mean_amount'] = origin_summary['total'] / origin_summary['count']
        origin_mean = origin_summary.sort_values('mean_amount', ascending=False).reset_index(drop=True)
        
        # Store results
        self.analysis_results['origin_mean'] = origin_mean
        
        # Create charts
        for top_n in [5, 10, 20, 30]:
            self.create_interactive_chart(
                origin_mean, 'origin', 'mean_amount',
                'Mean Amount per Origin',
                'origin_mean', top_n
            )
        
        # Store chart config
        self.chart_configs['origin_mean'] = {
            'title': 'Mean Amount per Origin',
            'data': origin_mean.to_dict('records')
        }
        
        print(f"[OK] Mean origin analysis complete")
        return origin_mean

    
    def save_to_excel(self, output_file=None):
        """Save all analysis results to a single Excel file with multiple sheets"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if output_file is None:
            output_file = self.output_dir / f"multiset_analysis_{timestamp}.xlsx"
        
        # Create Excel writer
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Sheet 1: Destinations by Count
            if 'dest_by_count' in self.analysis_results:
                df = self.analysis_results['dest_by_count']
                df.to_excel(writer, sheet_name='dest_by_count', index=False)
                
                # Add chart image if available
                img_path = self.charts_dir / 'dest_count_top20.png'
                if img_path.exists():
                    worksheet = writer.sheets['dest_by_count']
                    worksheet.insert_image('E2', str(img_path))
            
            # Sheet 2: Destinations by Amount
            if 'dest_by_amount' in self.analysis_results:
                df = self.analysis_results['dest_by_amount']
                df.to_excel(writer, sheet_name='dest_by_amount', index=False)
                
                # Add chart image if available
                img_path = self.charts_dir / 'dest_amount_top20.png'
                if img_path.exists():
                    worksheet = writer.sheets['dest_by_amount']
                    worksheet.insert_image('E2', str(img_path))
            
            # Sheet 3: Mean Amounts
            if 'dest_mean' in self.analysis_results:
                df = self.analysis_results['dest_mean']
                df.to_excel(writer, sheet_name='dest_mean', index=False)
                
                # Add chart image if available
                img_path = self.charts_dir / 'dest_mean_top20.png'
                if img_path.exists():
                    worksheet = writer.sheets['dest_mean']
                    worksheet.insert_image('E2', str(img_path))
            
            # Sheet 4: Red Flags
            if 'red_flags' in self.analysis_results:
                df = self.analysis_results['red_flags']
                df.to_excel(writer, sheet_name='red_flags', index=False)
                
                # Apply conditional formatting for red-flagged users
                worksheet = writer.sheets['red_flags']
                red_format = workbook.add_format({'bg_color': '#FF0000', 'font_color': '#FFFFFF'})
                
                # Apply formatting to rows where has_red_flag is True
                for row_num, has_flag in enumerate(df['has_red_flag'], start=1):
                    if has_flag:
                        worksheet.conditional_format(f'A{row_num+1}:E{row_num+1}', 
                                                    {'type': 'no_blanks', 'format': red_format})
            # Sheet 5: User Details
            if 'user_details' in self.analysis_results:
                df = self.analysis_results['user_details']
                df.to_excel(writer, sheet_name='user_details', index=False)

                worksheet = writer.sheets['user_details']
                workbook  = writer.book

                # Column index helpers (0-based)
                cols = {name: idx for idx, name in enumerate(df.columns)}
                red_col_idx = cols.get('Red_Flag', None)

                # 5 preferred light colors (cycled)
                palette = ['#ADD8E6',  # light blue
                        '#90EE90',  # light green
                        '#FFDAB9',  # light orange/peach
                        '#E6E6FA',  # light purple/lavender
                        '#FFFACD']  # light yellow (adds 5th)
                user_format_cache = {}
                bold_red = workbook.add_format({'bold': True, 'font_color': '#FF0000'})

                # Tint each row by User (same user -> same tint)
                for r in range(len(df)):
                    user = df.iloc[r]['User']
                    if user not in user_format_cache:
                        color = palette[len(user_format_cache) % len(palette)]
                        user_format_cache[user] = workbook.add_format({'bg_color': color})

                    # set_row uses 0-based row index; +1 to skip header row
                    worksheet.set_row(r + 1, cell_format=user_format_cache[user])

                    # Overwrite Red_Flag cell with bold red "Yes" if needed
                    if red_col_idx is not None and str(df.iloc[r]['Red_Flag']).strip() == 'Yes':
                        worksheet.write(r + 1, red_col_idx, 'Yes', bold_red)

            
            # Sheet 6: Operations
            if 'operations' in self.analysis_results:
                df = self.analysis_results['operations']
                df.to_excel(writer, sheet_name='operations', index=False)
            
            # Sheet 7: One to Many (colored by User)
            if 'one_to_many' in self.analysis_results:
                df = self.analysis_results['one_to_many']
                df.to_excel(writer, sheet_name='one_to_many', index=False)

                ws = writer.sheets['one_to_many']
                wb = writer.book

                # 5 light colors to cycle through (same user => same color)
                palette = [
                    '#ADD8E6',  # light blue
                    '#90EE90',  # light green
                    '#FFDAB9',  # light orange/peach
                    '#E6E6FA',  # light purple/lavender
                    '#FFFACD'   # light yellow
                ]
                fmt_cache = {}

                # Optional niceties
                ws.freeze_panes(1, 0)           # keep header fixed
                ws.autofilter(0, 0, len(df), len(df.columns)-1)

                # Tint each data row by its User (header is row 0; data starts at row 1)
                for r in range(len(df)):
                    user = str(df.iloc[r].get('User', ''))
                    if user not in fmt_cache:
                        color = palette[len(fmt_cache) % len(palette)]
                        fmt_cache[user] = wb.add_format({'bg_color': color})
                    ws.set_row(r + 1, cell_format=fmt_cache[user])

            # Sheet 7b: OtM-Summary (One-to-Many Summary)
            if 'OtM-Summary' in self.analysis_results:
                df = self.analysis_results['OtM-Summary']
                df.to_excel(writer, sheet_name='OtM-Summary', index=False)
                
                ws = writer.sheets['OtM-Summary']
                ws.freeze_panes(1, 0)
                ws.autofilter(0, 0, len(df), len(df.columns)-1)

            
            # Sheet 8: Many to One (colored by Receiver)
            if 'many_to_one' in self.analysis_results:
                df = self.analysis_results['many_to_one']
                df.to_excel(writer, sheet_name='many_to_one', index=False)

                ws = writer.sheets['many_to_one']
                wb = writer.book

                # 5 light colors (cycled). Same Receiver => same color.
                palette = [
                    '#ADD8E6',  # light blue
                    '#90EE90',  # light green
                    '#FFDAB9',  # light orange/peach
                    '#E6E6FA',  # light purple/lavender
                    '#FFFACD'   # light yellow
                ]
                fmt_cache = {}

                # Optional UX
                ws.freeze_panes(1, 0)
                ws.autofilter(0, 0, len(df), len(df.columns) - 1)

                # Tint each data row by its Receiver (header is row 0; data starts at row 1)
                for r in range(len(df)):
                    receiver = str(df.iloc[r].get('Receiver', ''))
                    if receiver not in fmt_cache:
                        color = palette[len(fmt_cache) % len(palette)]
                        fmt_cache[receiver] = wb.add_format({'bg_color': color})
                    ws.set_row(r + 1, cell_format=fmt_cache[receiver])

            # Sheet 8b: MtO-Summary (Many-to-One Summary)
            if 'MtO-Summary' in self.analysis_results:
                df = self.analysis_results['MtO-Summary']
                df.to_excel(writer, sheet_name='MtO-Summary', index=False)
                
                ws = writer.sheets['MtO-Summary']
                ws.freeze_panes(1, 0)
                ws.autofilter(0, 0, len(df), len(df.columns)-1)

            
            # Sheet 9: Geometric Patterns
            if 'geometric_patterns' in self.analysis_results:
                df = self.analysis_results['geometric_patterns']
                df.to_excel(writer, sheet_name='geometric_patterns', index=False)

            # Sheet 10: Origins by Count
            if 'origin_by_count' in self.analysis_results:
                df = self.analysis_results['origin_by_count']
                df.to_excel(writer, sheet_name='origin_by_count', index=False)
                
                # Add chart image if available
                img_path = self.charts_dir / 'origin_count_top20.png'
                if img_path.exists():
                    worksheet = writer.sheets['origin_by_count']
                    worksheet.insert_image('E2', str(img_path))
            
            # Sheet 11: Origins by Amount
            if 'origin_by_amount' in self.analysis_results:
                df = self.analysis_results['origin_by_amount']
                df.to_excel(writer, sheet_name='origin_by_amount', index=False)
                
                # Add chart image if available
                img_path = self.charts_dir / 'origin_amount_top20.png'
                if img_path.exists():
                    worksheet = writer.sheets['origin_by_amount']
                    worksheet.insert_image('E2', str(img_path))
            
            # Sheet 12: Mean Origin Amounts
            if 'origin_mean' in self.analysis_results:
                df = self.analysis_results['origin_mean']
                df.to_excel(writer, sheet_name='origin_mean', index=False)
                
                # Add chart image if available
                img_path = self.charts_dir / 'origin_mean_top20.png'
                if img_path.exists():
                    worksheet = writer.sheets['origin_mean']
                    worksheet.insert_image('E2', str(img_path))
            
            # Add summary sheet
            summary_data = {
                'Analysis': [],
                'Records': [],
                'Status': []
            }
            
            analysis_names = {
                'dest_by_count': 'Destinations by Count',
                'dest_by_amount': 'Destinations by Amount',
                'dest_mean': 'Mean Amounts',
                'red_flags': 'User Red Flags',
                'user_details': 'User Details',
                'operations': 'Operations Analysis',
                'one_to_many': 'One-to-Many',
                'OtM-Summary': 'One-to-Many Summary',
                'many_to_one': 'Many-to-One',
                'MtO-Summary': 'Many-to-One Summary',
                'geometric_patterns': 'Geometric Patterns',
                'origin_by_count': 'Origins by Count',        
                'origin_by_amount': 'Origins by Amount',      
                'origin_mean': 'Mean Origin Amounts'          
            }
            
            for key, name in analysis_names.items():
                if key in self.analysis_results:
                    df = self.analysis_results[key]
                    summary_data['Analysis'].append(name)
                    summary_data['Records'].append(len(df) if not df.empty else 0)
                    summary_data['Status'].append('Complete')
                else:
                    summary_data['Analysis'].append(name)
                    summary_data['Records'].append(0)
                    summary_data['Status'].append('Not Run')
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Format summary sheet
            worksheet = writer.sheets['Summary']
            header_format = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3'})
            for col_num, value in enumerate(summary_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
        
        print(f"[OK] Results saved to: {output_file}")
        return str(output_file)


    def run_all_analyses(self):
        """Run all analyses in sequence"""
        print("\n" + "="*70)
        print("[EXECUTE] RUNNING ALL MULTISET ANALYSES")
        print("="*70)
        
        analyses = [
            ("1. Unique Destinations", self.analyze_unique_destinations),
            ("2. Mean Destination Amounts", self.analyze_mean_amounts),
            ("3. Unique Origins", self.analyze_unique_origins),          
            ("4. Mean Origin Amounts", self.analyze_mean_origin_amounts),
            ("5. User Red Flags", self.analyze_user_red_flags),
            ("6. User Details", self.analyze_user_details),
            ("7. Operation Analysis", self.analyze_operations),
            ("8. One-to-Many", self.analyze_one_to_many),
            ("9. Many-to-One", self.analyze_many_to_one),
            ("10. Geometric Patterns", self.analyze_geometric_patterns)
        ]
        
        results = {}
        for name, analysis_func in analyses:
            print(f"\nRunning {name}...")
            try:
                result = analysis_func()
                results[name] = "Complete" if result is not None else "No Data"
            except Exception as e:
                print(f"[ERROR] in {name}: {e}")
                results[name] = f"Error: {e}"
        
        # Save all results to single Excel file
        output_file = self.save_to_excel()
        
        print("\n" + "="*70)
        print("[SUMMARY] ANALYSIS COMPLETE")
        print("="*70)
        
        for name, status in results.items():
            print(f"{name}: {status}")
        
        print(f"\n[FILE] Results saved to: {output_file}")
        
        return output_file, self.chart_configs
    


def main():
    """Main function to run the multiset analyzer"""
    print("\n" + "="*70)
    print("MULTISET ANALYZER SYSTEM - REFACTORED")
    print("="*70)
    
    # Initialize analyzer
    analyzer = MultisetAnalyzer()
    
    # Load datasets
    print("\n[FILES] Loading datasets...")
    if not analyzer.load_datasets():
        print("[ERROR] Failed to load datasets")
        return None, None
    
    # Run all analyses
    excel_file, chart_configs = analyzer.run_all_analyses()
    
    print("\n[OK] Analysis complete!")
    return excel_file, chart_configs

if __name__ == "__main__":
    main()