import tkinter as tk
from tkinter import messagebox
from functools import partial
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
from simulator import Simulator
import time

class JobSchedulerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Job Scheduler")

        # UI Elements
        self.job_label = tk.Label(root, text="Press to step through events...", font=("Arial", 14))
        self.job_label.pack(pady=10)

        self.step_button = tk.Button(root, text="Step", command=self._step, font=("Arial", 12))
        self.step_button.pack(pady=20)

        self.step50_button = tk.Button(root, text="Step (50)", command=self._step50, font=("Arial", 12))
        self.step50_button.pack(pady=20)

        self.step1k_button = tk.Button(root, text="Step (1000)", command=self._step1K, font=("Arial", 12))
        self.step1k_button.pack(pady=20)


        # Graph frame
        self.graph_frame = tk.Frame(root)
        self.graph_frame.pack(pady=20)

        self.fig, self.axs = plt.subplots(1, 2, figsize=(10, 4))
        self.fig.suptitle("Utilization & Avg Wait Time", fontsize=14)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.graph_frame)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack()

        self.s = Simulator()
        # self.s.read_data('../data/pbs/input/job_log.swf', '../data/pbs/input/system.json')
        # self.s.initialize('../data/pbs/output')
        self.s.read_data('../data/theta22/input/theta22.swf', '../data/theta22/input/system.json')
        self.s.initialize('../data/theta22/output')
        self.utilization = []
        self.avg_wait = []
        self.timesteps = []

    def _step(self):
        print('step')
        try:
            self.s.step()
            observation = self.s.observe()
            # print(observation)
            self.utilization.append(observation['utilization'])
            self.avg_wait.append(observation['avg_wait'])
            self.timesteps.append(observation['timestamp'])
            self.update_graph()

        except Exception as e:
            print(e)
            self.s.cleanup()

    def _step50(self):
        print('step50')
        try:
            for i in range(0, 50):
                self.s.step()
                observation = self.s.observe()
                # print(observation)
                self.utilization.append(observation['utilization'])
                self.avg_wait.append(observation['avg_wait'])
                self.timesteps.append(observation['timestamp'])
                self.update_graph()

        except Exception as e:
            print(e)
            self.s.cleanup()


    def _step1K(self):
        print('step1000')
        try:
            for i in range(0, 1000):
                self.s.step()
                observation = self.s.observe()
                # print(observation)
                self.utilization.append(observation['utilization'])
                self.avg_wait.append(observation['avg_wait'])
                self.timesteps.append(observation['timestamp'])
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
        self.axs[0].set_xlabel("Timestep")
        self.axs[0].set_ylabel("Utilization")
        self.axs[0].set_title("Resource Utilization")

        # You can add another plot to axs[1] if needed
        self.axs[1].plot(self.timesteps, self.avg_wait)
        self.axs[1].set_xlabel("Timestep")
        self.axs[1].set_ylabel("Average Wait Time (s)")
        self.axs[1].set_title("Average Wait Time")

        # Redraw the canvas
        self.canvas.draw()
        time.sleep(1)




# Main Script
if __name__ == '__main__':
    # Start GUI
    root = tk.Tk()
    app = JobSchedulerGUI(root)
    root.mainloop()