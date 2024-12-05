import tkinter as tk
from tkinter import messagebox
from functools import partial
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd

# GUI Class
class JobSchedulerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Job Scheduler")

        # UI Elements
        self.job_label = tk.Label(root, text="Job Details", font=("Arial", 14))
        self.job_label.pack(pady=10)

        self.step_button = tk.Button(root, text="Step", command=self._step, font=("Arial", 12))
        self.step_button.pack(pady=20)


        # Utilization graph frame
        self.graph_frame = tk.Frame(root)
        self.graph_frame.pack(pady=20)

        self.fig, self.axs = plt.subplots(1, 1, figsize=(10, 4))
        self.fig.suptitle("Cluster Utilization", fontsize=14)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.graph_frame)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack()

    def _step(self):
        try:
            print('Pressed step!')
        except Exception as e:
            messagebox.showinfo("Errors", "Error")
            self.step_button.config(state=tk.DISABLED)

    def update_utilization_graph(self, current_timestamp):
        theta_utilization = {}
        print(theta_utilization)
        polaris_utilization = {}
        print(polaris_utilization)

        # Clear previous plots
        self.axs[0].clear()
        self.axs[1].clear()

        # Plot Polaris utilization
        self.axs[0].plot(polaris_utilization['timestamp'], polaris_utilization['utilization'], label='Polaris Utilization', color='blue')
        self.axs[0].set_title("Polaris Utilization")
        self.axs[0].set_xlabel("Timestamp")
        self.axs[0].set_ylabel("Utilization")
        
        # Dynamically adjust y-limits based on data
        if not polaris_utilization['utilization'].empty:
            polaris_min = polaris_utilization['utilization'].min()
            polaris_max = polaris_utilization['utilization'].max()
            self.axs[0].set_ylim([max(0, polaris_min - 0.1), min(1.0, polaris_max + 0.1)])
        else:
            self.axs[0].set_ylim(0, 1.0)

        self.axs[0].legend()

        # Plot Theta utilization
        self.axs[1].plot(theta_utilization['timestamp'], theta_utilization['utilization'], label='Theta Utilization', color='green')
        self.axs[1].set_title("Theta Utilization")
        self.axs[1].set_xlabel("Timestamp")
        self.axs[1].set_ylabel("Utilization")
        
        # Dynamically adjust y-limits based on data
        if not theta_utilization['utilization'].empty:
            theta_min = theta_utilization['utilization'].min()
            theta_max = theta_utilization['utilization'].max()
            self.axs[1].set_ylim([max(0, theta_min - 0.1), min(1.0, theta_max + 0.1)])
        else:
            self.axs[1].set_ylim(0, 1.0)

        self.axs[1].legend()

        # Redraw the plot
        self.canvas.draw()

    def assign_job_to_cluster(self, cluster_name):

        # Simulate the selected job
        messagebox.showinfo("Assigned to cluster")

        # Reset buttons and UI for the next job
        self.polaris_button.config(state=tk.DISABLED)
        self.theta_button.config(state=tk.DISABLED)
        self.turnaround_label.config(text="Turnaround Times")
        self.next_button.config(state=tk.NORMAL)


# Main Script
if __name__ == '__main__':
    # Start GUI
    root = tk.Tk()
    app = JobSchedulerGUI(root)
    root.mainloop()