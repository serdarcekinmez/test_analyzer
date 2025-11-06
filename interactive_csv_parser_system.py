




import pandas as pd
import csv
import tkinter as tk
from tkinter import filedialog, messagebox
import warnings
import io
import os
import pickle
import json
from datetime import datetime
from pathlib import Path
warnings.filterwarnings("ignore")

# ============================================================
#  DATA MANAGEMENT SYSTEM FOR MULTI-MODULE ARCHITECTURE
# ============================================================

class DatasetManager:
    """
    Central data management system for storing and accessing parsed datasets.
    Provides interface for multiset_analyzer.py and multiset_insights.py
    """
    
    def __init__(self, storage_dir="parsed_datasets"):
        """Initialize the data manager with storage directory"""
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(exist_ok=True)
        
        # In-memory storage
        self.datasets = {}
        self.metadata = {
            'parse_date': None,
            'files_processed': [],
            'dataset_count': 0,
            'dataset_info': {}
        }
        
        # Session ID for unique storage
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def add_dataset(self, name, dataframe, source_file):
        """Add a dataset to the manager"""
        self.datasets[name] = dataframe
        self.metadata['dataset_info'][name] = {
            'shape': dataframe.shape,
            'columns': list(dataframe.columns),
            'source': source_file,
            'type': name.rstrip('0123456789')  # Extract type (Exits, Inputs, Waves)
        }
        
    def save_all(self):
        """Save all datasets to disk for persistence"""
        session_dir = self.storage_dir / f"session_{self.session_id}"
        session_dir.mkdir(exist_ok=True)
        
        # Save as pickle for Python access
        pickle_file = session_dir / "datasets.pkl"
        with open(pickle_file, 'wb') as f:
            pickle.dump(self.datasets, f)
        
        # Save metadata as JSON
        meta_file = session_dir / "metadata.json"
        # Convert metadata to JSON-serializable format
        meta_save = self.metadata.copy()
        meta_save['parse_date'] = self.session_id
        with open(meta_file, 'w') as f:
            json.dump(meta_save, f, indent=2)
        
        # Save individual CSVs for portability
        csv_dir = session_dir / "csv_files"
        csv_dir.mkdir(exist_ok=True)
        for name, df in self.datasets.items():
            csv_file = csv_dir / f"{name}.csv"
            df.to_csv(csv_file, index=False)
        
        print(f"\n[SAVE] Data saved to: {session_dir}")
        return session_dir
    
    def load_session(self, session_id=None):
        """Load a previous session's datasets"""
        if session_id is None:
            # Get the most recent session
            sessions = sorted([d for d in self.storage_dir.iterdir() if d.is_dir()])
            if not sessions:
                return False
            session_dir = sessions[-1]
        else:
            session_dir = self.storage_dir / f"session_{session_id}"
        
        if not session_dir.exists():
            return False
        
        # Load datasets
        pickle_file = session_dir / "datasets.pkl"
        if pickle_file.exists():
            with open(pickle_file, 'rb') as f:
                self.datasets = pickle.load(f)
        
        # Load metadata
        meta_file = session_dir / "metadata.json"
        if meta_file.exists():
            with open(meta_file, 'r') as f:
                self.metadata = json.load(f)
        
        return True
    
    def get_dataset(self, name):
        """Get a specific dataset by name"""
        return self.datasets.get(name)
    
    def get_datasets_by_type(self, dataset_type):
        """Get all datasets of a specific type (Exits, Inputs, Waves)"""
        return {name: df for name, df in self.datasets.items() 
                if name.startswith(dataset_type)}
    
    def merge_by_type(self, dataset_type):
        """Merge all datasets of the same type"""
        matching = self.get_datasets_by_type(dataset_type)
        if matching:
            return pd.concat(matching.values(), ignore_index=True)
        return None
    
    def get_summary(self):
        """Get summary information about all datasets"""
        summary = []
        for name, info in self.metadata['dataset_info'].items():
            summary.append({
                'Name': name,
                'Type': info['type'],
                'Rows': info['shape'][0],
                'Columns': info['shape'][1],
                'Source': os.path.basename(info['source'])
            })
        return pd.DataFrame(summary)

# ============================================================
#  INTERACTIVE CSV PARSER WITH FILE SELECTION DIALOG
# ============================================================

class InteractiveCSVParser:
    """Interactive CSV parser with user-friendly file selection"""
    
    def __init__(self):
        self.data_manager = DatasetManager()
        self.file_paths = []
        self.file_count = 0
        
    def clean_dataframe(self, df):
        """Clean dataframe by removing quotes and stripping whitespace"""
        df = df.map(lambda x: x.strip().strip('"').strip("'") if isinstance(x, str) else x)
        df = df.replace({"": None, "nan": None, "NaN": None, "None": None, "\xa0": None})
        
        if len(df.columns) > 0:
            df.columns = (
                df.columns.astype(str)
                .str.replace('"', '', regex=False)
                .str.replace("'", '', regex=False)
                .str.strip()
            )
        return df
    
    def detect_separator(self, file_path):
        """Auto-detect the separator"""
        with open(file_path, "r", encoding="utf-8-sig") as f:
            first_line = f.readline()
        return "\t" if first_line.count("\t") > first_line.count(",") else ","
    
    # ============================================================
    #  PARSING METHODS (integrated with DataManager)
    # ============================================================
    
    def parse_exits_dataset(self, file_path, file_num):
        """Parse the Exits dataset"""
        print(f"  [STATS] Parsing Exits{file_num}...")
        
        sep = self.detect_separator(file_path)
        
        # Load from row 4
        df = pd.read_csv(
            file_path, skiprows=3, header=0, sep=sep,
            encoding="utf-8-sig", on_bad_lines="skip",
            quoting=csv.QUOTE_NONE, escapechar="\\", dtype=str
        )
        
        df = self.clean_dataframe(df)
        
        # Find end of Exits
        exits_end = self._find_dataset_end(df)
        
        # Extract Exits dataset
        exits_df = df.iloc[:exits_end + 1].copy()
        if exits_df.shape[1] > 2:
            exits_df = exits_df.iloc[:, :-2]
        
        # Rename columns
        exits_df.columns = [chr(65 + i) for i in range(len(exits_df.columns))]
        exits_df.dropna(how="all", inplace=True)
        exits_df.reset_index(drop=True, inplace=True)
        
        # Store in data manager
        self.data_manager.add_dataset(f'Exits{file_num}', exits_df, file_path)
        print(f"      [OK] {exits_df.shape[0]} rows × {exits_df.shape[1]} columns")
        
        return exits_end + 3  # Return absolute row position
    
    def parse_inputs_dataset(self, file_path, exits_end_row, file_num):
        """Parse the Inputs dataset"""
        print(f"  [STATS] Parsing Inputs{file_num}...")
        
        sep = self.detect_separator(file_path)
        
        # Find Inputs start
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
        
        inputs_start = None
        search_start = exits_end_row + 10
        search_end = min(search_start + 20, len(lines))
        
        for i in range(search_start, search_end):
            if i < len(lines):
                fields = lines[i].split(sep)
                non_empty = sum(1 for f in fields if f.strip().strip('"\''))
                if non_empty >= 5:
                    inputs_start = i
                    break
        
        if inputs_start is None:
            inputs_start = exits_end_row + 16
        
        # Read Inputs
       
        df_inputs = pd.read_csv(
            file_path, skiprows=inputs_start, header=0, sep=sep,
            encoding="utf-8-sig", on_bad_lines="skip",
            quoting=csv.QUOTE_NONE, escapechar="\\", dtype=str,
            index_col=False  
        )

        
        df_inputs = self.clean_dataframe(df_inputs)
        
        # Find end
        inputs_end = self._find_dataset_end(df_inputs)
        if inputs_end < len(df_inputs) - 1:
            df_inputs = df_inputs.iloc[:inputs_end + 1]
        
        # Process
        if df_inputs.shape[1] > 2:
            df_inputs = df_inputs.iloc[:, :-2]
        df_inputs.dropna(how="all", inplace=True)
        df_inputs.reset_index(drop=True, inplace=True)
        
        # Rename columns
        phonetic = ["Alpha", "Beta", "Charlie", "Delta","Delta-1", "Echo", "Foxtrot", "Golf", "Hotel",
                   "India", "Juliett", "Kilo", "Lima", "Mike", "November", "Oscar", "Papa",
                   "Quebec", "Romeo", "Sierra", "Tango", "Uniform", "Victor", "Whiskey", "Xray"]
        df_inputs.columns = (phonetic + [f"Col_{i}" for i in range(24, 100)])[:len(df_inputs.columns)]
        
        # Store in data manager
        self.data_manager.add_dataset(f'Inputs{file_num}', df_inputs, file_path)
        print(f"      [OK] {df_inputs.shape[0]} rows × {df_inputs.shape[1]} columns")
    
    def parse_waves_dataset(self, file_path, file_num):
        """Parse the Waves dataset"""
        print(f"  [STATS] Parsing Waves{file_num}...")
        
        sep = self.detect_separator(file_path)
        
        # Find Waves header
        with open(file_path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()
        
        # Search for "RAISON DU RENVOI"
        tail_start = max(0, len(lines) - 800)
        waves_header = None
        
        for i in range(len(lines) - 1, tail_start - 1, -1):
            if "raison du renvoi" in lines[i].lower().replace('"', '').replace("'", ""):
                waves_header = i
                break
        
        # Fallback: empty first 3 columns
        if waves_header is None:
            tail_text = "".join(lines[tail_start:])
            df_tail = pd.read_csv(
                io.StringIO(tail_text), header=None, sep=sep,
                on_bad_lines="skip", quoting=csv.QUOTE_NONE, dtype=str
            )
            df_tail = self.clean_dataframe(df_tail)
            
            for i in range(len(df_tail) - 1, -1, -1):
                if df_tail.shape[1] >= 3:
                    first3 = df_tail.iloc[i, :3]
                    if pd.isna(first3).all() or (first3.astype(str).str.strip() == "").all():
                        waves_header = tail_start + i + 1
                        break
        
        if waves_header is None:
            waves_header = max(0, len(lines) - 50)
        
        # Read Waves
        waves_df = pd.read_csv(
            file_path, skiprows=waves_header, header=0, sep=sep,
            encoding="utf-8-sig", on_bad_lines="skip",
            quoting=csv.QUOTE_NONE, escapechar="\\", dtype=str, index_col=False
        )
        
        waves_df = self.clean_dataframe(waves_df)
        
        # Keep first 8 columns
        waves_df = waves_df.iloc[:, :8] if waves_df.shape[1] >= 8 else waves_df
        waves_df.columns = [f"S{i+1}" for i in range(len(waves_df.columns))]
        waves_df.dropna(how="all", inplace=True)
        waves_df.reset_index(drop=True, inplace=True)
        
        # Store in data manager
        self.data_manager.add_dataset(f'Waves{file_num}', waves_df, file_path)
        print(f"      [OK] {waves_df.shape[0]} rows × {waves_df.shape[1]} columns")
    
    def _find_dataset_end(self, df):
        """Helper to find dataset end"""
        # Look for "Totale"
        date_col = next((c for c in df.columns if "date" in str(c).lower()), None)
        if date_col:
            totale_idx = df[df[date_col].astype(str).str.strip().str.lower() == "totale"].index
            if len(totale_idx) > 0:
                return totale_idx[0] - 2
        
        # Look for empty first 3 columns
        for idx in range(len(df)):
            if df.shape[1] >= 3:
                first_three = df.iloc[idx, :3]
                if pd.isna(first_three).all() or (first_three.astype(str).str.strip() == "").all():
                    return idx - 1
        
        return len(df) - 1
    
    # ============================================================
    #  INTERACTIVE FILE SELECTION
    # ============================================================
    
    def interactive_file_selection(self):
        """Interactive file selection with Yes/No dialog"""
        print("\n" + "="*70)
        print("[INFO] INTERACTIVE FILE SELECTION")
        print("="*70)
        
        while True:
            # Ask if user wants to add a file
            root = tk.Tk()
            root.withdraw()
            root.attributes('-topmost', True)
            
            # Create custom message with current status
            if self.file_paths:
                message = f"Currently selected: {len(self.file_paths)} file(s)\n\nDo you want to add another dataset file?"
            else:
                message = "No files selected yet.\n\nDo you want to add a dataset file?"
            
            result = messagebox.askyesno(
                "Add Dataset File",
                message,
                icon='question'
            )
            
            if result:  # User clicked Yes
                # File selection dialog
                file_path = filedialog.askopenfilename(
                    title=f"Select CSV file #{len(self.file_paths) + 1}",
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                    parent=root
                )
                
                if file_path:  # User selected a file
                    self.file_paths.append(file_path)
                    print(f"[ok] Added: {os.path.basename(file_path)}")
                else:  # User cancelled the file dialog
                    print(" [info] File selection cancelled")
                
                root.destroy()
                
            else:  # User clicked No
                root.destroy()
                break
        
        if not self.file_paths:
            print(" No files selected. Exiting.")
            return False
        
        print(f"\n [Stats] Total files to process: {len(self.file_paths)}")
        for i, fp in enumerate(self.file_paths, 1):
            print(f"  {i}. {os.path.basename(fp)}")
        
        return True
    
    # ============================================================
    #  MAIN PROCESSING
    # ============================================================
    
    def process_all_files(self):
        """Process all selected files"""
        print("\n" + "="*70)
        print("⚙️ PROCESSING FILES")
        print("="*70)
        
        for file_num, file_path in enumerate(self.file_paths, 1):
            print(f"\n📁 Processing file #{file_num}: {os.path.basename(file_path)}")
            print("-"*50)
            
            try:
                # Parse all three datasets
                exits_end = self.parse_exits_dataset(file_path, file_num)
                self.parse_inputs_dataset(file_path, exits_end, file_num)
                self.parse_waves_dataset(file_path, file_num)
                
                # Update metadata
                self.data_manager.metadata['files_processed'].append(file_path)
                self.file_count = file_num
                
                print(f"[OK] File #{file_num} completed successfully")
                
            except Exception as e:
                print(f"❌ Error processing file #{file_num}: {e}")
                continue
        
        self.data_manager.metadata['dataset_count'] = len(self.data_manager.datasets)
        self.data_manager.metadata['parse_date'] = datetime.now().isoformat()
    
    def display_summary(self):
        """Display summary of parsed datasets"""
        print("\n" + "="*70)
        print("[OK] PARSING COMPLETE - SUMMARY")
        print("="*70)
        
        summary_df = self.data_manager.get_summary()
        if not summary_df.empty:
            try:
                from IPython.display import display
                display(summary_df)
            except:
                print(summary_df.to_string(index=False))
        
        print(f"\nTotal files processed: {self.file_count}")
        print(f"Total datasets extracted: {len(self.data_manager.datasets)}")
        
        # Save data
        save_dir = self.data_manager.save_all()
        
        print("\n" + "="*70)
        print(" [HINT] DATA ACCESS INFORMATION")
        print("="*70)
        print(f"Session ID: {self.data_manager.session_id}")
        print(f"Storage location: {save_dir}")
        print("\nFor multiset_analyzer.py and multiset_insights.py:")
        print("  from parsed_datasets import load_datasets")
        print("  data_manager = load_datasets()")
        print("  # or")
        print(f"  data_manager = load_datasets('{self.data_manager.session_id}')")
        
        return self.data_manager

# ============================================================
#  API FOR OTHER MODULES
# ============================================================

def load_datasets(session_id=None):
    """
    Load parsed datasets for use in other modules.
    This function can be imported by multiset_analyzer.py and multiset_insights.py
    
    Args:
        session_id: Specific session to load (optional)
    
    Returns:
        DatasetManager object with all datasets
    """
    manager = DatasetManager()
    if manager.load_session(session_id):
        print(f"[OK] Loaded {len(manager.datasets)} datasets")
        return manager
    else:
        print("[ERROR] No saved datasets found")
        return None

def get_latest_datasets():
    """Quick function to get the most recent datasets"""
    return load_datasets()

# ============================================================
#  MAIN EXECUTION
# ============================================================

def main():
    """Main function to run the interactive parser"""
    
    parser = InteractiveCSVParser()
    
    # Interactive file selection
    if not parser.interactive_file_selection():
        return None
    
    # Process all files
    parser.process_all_files()
    
    # Display summary
    data_manager = parser.display_summary()
    
    # Make datasets available globally for immediate use
    globals().update(data_manager.datasets)
    
    print("\n[OK] Datasets are now available as variables:")
    for name in data_manager.datasets.keys():
        print(f"  • {name}")
    
    return data_manager

if __name__ == "__main__":
    # Run the interactive parser
    data_manager = main()
    
    # Example of how to access data
    if data_manager:
        print("\n[INFO] Quick access examples:")
        print("  exits1 = data_manager.get_dataset('Exits1')")
        print("  all_exits = data_manager.merge_by_type('Exits')")
        print("  summary = data_manager.get_summary()")