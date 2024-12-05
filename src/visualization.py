import tkinter as tk
from tkinter import messagebox
from functools import partial
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
from simulator import Simulator


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

        self.fig, self.axs = plt.subplots(1, 2, figsize=(10, 4))
        self.fig.suptitle("Cluster Utilization", fontsize=14)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.graph_frame)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack()

        self.s = Simulator()
        self.s.read_data('../data/theta22/input/theta22.swf', '../data/theta22/input/system.json')
        self.s.initialize('../data/theta22/output')
        self.utilization = []
        self.timesteps = []

    def _step(self):
        print('step')
        try:
            self.s.step()
            observation = self.s.observe()
            self.utilization.append(observation['utilization'])
            self.timesteps.append(observation['timestamp'])
            print(observation)

            self.update_graph()

        except Exception as e:
            print(e)
            self.s.cleanup()

    def update_graph(self):
        # Clear previous plots
        self.axs[0].clear()
        self.axs[1].clear()

        # Plot utilization over time
        self.axs[0].plot(self.timesteps, self.utilization)
        self.axs[0].set_xlabel("Time")
        self.axs[0].set_ylabel("Utilization")
        self.axs[0].set_title("Utilization over Time")

        # You can add another plot to axs[1] if needed

        # Redraw the canvas
        self.canvas.draw()


    def update_utilization_graph(self):



        # Clear previous plots
        self.axs[0].clear()
        # self.axs[1].clear()

        # Plot Polaris utilization
        self.axs[0].plot(self.timesteps, self.utilization, label='System Utilization', color='blue')
        self.axs[0].set_title("System Utilization")
        self.axs[0].set_xlabel("Timestamp")
        self.axs[0].set_ylabel("Utilization")

        if not self.axs[0].lines:  # Check if the line exists
            self.line, = self.axs[0].plot(self.timesteps, self.utilization, label='System Utilization', color='blue')
        else:
            self.line.set_data(self.timesteps, self.utilization)  # Update existing line
        
        # Dynamically adjust y-limits based on data
        if len(self.utilization) == 0:
            polaris_min = self.utilization.min()
            polaris_max = self.utilization.max()
            self.axs[0].set_ylim([max(0, polaris_min - 0.1), min(1.0, polaris_max + 0.1)])
        else:
            self.axs[0].set_ylim(0, 1.0)

        self.axs[0].legend()

        # # Plot Theta utilization
        # self.axs[1].plot(theta_utilization['timestamp'], theta_utilization['utilization'], label='Theta Utilization', color='green')
        # self.axs[1].set_title("Theta Utilization")
        # self.axs[1].set_xlabel("Timestamp")
        # self.axs[1].set_ylabel("Utilization")
        
        # # Dynamically adjust y-limits based on data
        # if not theta_utilization['utilization'].empty:
        #     theta_min = theta_utilization['utilization'].min()
        #     theta_max = theta_utilization['utilization'].max()
        #     self.axs[1].set_ylim([max(0, theta_min - 0.1), min(1.0, theta_max + 0.1)])
        # else:
        #     self.axs[1].set_ylim(0, 1.0)

        # self.axs[1].legend()

        # Redraw the plot
        self.canvas.draw()


# Main Script
if __name__ == '__main__':
    # Start GUI
    root = tk.Tk()
    app = JobSchedulerGUI(root)
    root.mainloop()