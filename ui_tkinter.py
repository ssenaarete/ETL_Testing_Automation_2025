import sys
import os
# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from utils.config_loader import ConfigLoader
from Test_cases.test_manager import TEST_CASES, run_test

class ETLTestDashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("🚀 ETL Test Automation Dashboard")
        self.geometry("800x600")

        # --- Dropdown for test cases ---
        self.label = tk.Label(self, text="Select a Test Case:", font=("Arial", 12))
        self.label.pack(pady=10)

        self.selected_test = tk.StringVar()
        self.combo = ttk.Combobox(self, textvariable=self.selected_test, values=list(TEST_CASES.keys()), state="readonly", width=50)
        self.combo.pack(pady=5)
        self.combo.current(0)

        # --- Run Buttons ---
        self.run_single_btn = tk.Button(self, text="▶ Run Selected Test", command=self.run_selected_test, bg="green", fg="white", font=("Arial", 10))
        self.run_single_btn.pack(pady=5)

        self.run_all_btn = tk.Button(self, text="▶ Run All Tests", command=self.run_all_tests, bg="blue", fg="white", font=("Arial", 10))
        self.run_all_btn.pack(pady=5)

        # --- Output Text Area ---
        self.output_label = tk.Label(self, text="Logs / Results:", font=("Arial", 12))
        self.output_label.pack(pady=10)

        self.output_area = scrolledtext.ScrolledText(self, wrap=tk.WORD, width=90, height=20, font=("Courier", 9))
        self.output_area.pack(padx=10, pady=5)

    # config_loader = ConfigLoader()

    def run_selected_test(self):
        test_name = self.selected_test.get()
        self.log(f"▶ Running {test_name}...")
        try:
            result = run_test(test_name)
            self.log(f"✅ {test_name} completed successfully.\nResult:\n{result}\n")
            messagebox.showinfo("Success", f"{test_name} completed successfully!")
        except Exception as e:
            self.log(f"❌ Error in {test_name}: {str(e)}\n")
            # messagebox.showerror("Error", f"{test_name} failed: {str(e)}")
            messagebox.showerror("Error", f"{test_name} failed")

    def run_all_tests(self):
        self.log("▶ Running all test cases...\n")
        results = {}
        for test_name in TEST_CASES.keys():
            try:
                result = run_test(test_name)
                results[test_name] = result
                self.log(f"✅ {test_name} completed.\n")
            except Exception as e:
                results[test_name] = f"Error: {str(e)}"
                self.log(f"❌ {test_name} failed: {str(e)}\n")
        messagebox.showinfo("Completed", "All test cases executed. Check logs for details.")
        self.log(f"All Results:\n{results}\n")

    def log(self, text):
        self.output_area.insert(tk.END, text + "\n")
        self.output_area.see(tk.END)

if __name__ == "__main__":
    app = ETLTestDashboard()
    app.mainloop()
