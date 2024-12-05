import tkinter as tk
from tkinter import messagebox
from functools import partial
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd
from CqSim.Cqsim_plus import Cqsim_plus
from utils import disable_print
from trace_utils import read_swf_generator, num_jobs_swf

# Function to read and process ULT file
def read_ult(path):
    data = []
    with open(f'{path}', 'r') as f:
        for line in f:
            parts = line.strip().split(';')
            timestamp1, event_type, timestamp2 = parts[0:3]
            metrics = dict(item.split('=') for item in parts[3].split())
            data.append(
                [
                    float(timestamp1),
                    event_type,
                    float(timestamp2),
                    float(metrics['uti']),
                    float(metrics['waitNum']),
                    float(metrics['waitSize'])
                ]
            )

    df = pd.DataFrame(data, columns=['timestamp', 'event_type', 'timestamp2', 'utilization', 'waitNum', 'waitSize'])
    df_sorted = df.sort_values(by='timestamp')
    return df_sorted

# GUI Class
class JobSchedulerGUI:
    def __init__(self, root, job_generator, cqp, polaris, theta, polaris_ult, theta_ult):
        self.root = root
        self.root.title("Job Scheduler")

        # Job-related variables
        self.job_generator = job_generator
        self.cqp = cqp
        self.polaris = polaris
        self.theta = theta
        self.polaris_ult = polaris_ult
        self.theta_ult = theta_ult
        self.current_job = None

        # UI Elements
        self.job_label = tk.Label(root, text="Job Details", font=("Arial", 14))
        self.job_label.pack(pady=10)

        self.turnaround_label = tk.Label(root, text="Turnaround Times", font=("Arial", 12))
        self.turnaround_label.pack(pady=5)

        self.next_button = tk.Button(root, text="Next Job", command=self.process_next_job, font=("Arial", 12))
        self.next_button.pack(pady=20)

        self.cluster_buttons_frame = tk.Frame(root)
        self.cluster_buttons_frame.pack(pady=10)

        self.polaris_button = tk.Button(
            self.cluster_buttons_frame, text="Assign to Polaris", font=("Arial", 12),
            command=partial(self.assign_job_to_cluster, "Polaris")
        )
        self.polaris_button.pack(side=tk.LEFT, padx=10)

        self.theta_button = tk.Button(
            self.cluster_buttons_frame, text="Assign to Theta", font=("Arial", 12),
            command=partial(self.assign_job_to_cluster, "Theta")
        )
        self.theta_button.pack(side=tk.LEFT, padx=10)

        # Utilization graph frame
        self.graph_frame = tk.Frame(root)
        self.graph_frame.pack(pady=20)

        self.fig, self.axs = plt.subplots(1, 2, figsize=(10, 4))
        self.fig.suptitle("Cluster Utilizations", fontsize=14)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.graph_frame)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack()

        # Disable cluster buttons initially
        self.polaris_button.config(state=tk.DISABLED)
        self.theta_button.config(state=tk.DISABLED)

    def process_next_job(self):
        try:
            # Fetch the next job from the generator
            self.current_job = next(self.job_generator)
            job_id = self.current_job['id']
            req_proc = self.current_job['req_proc']
            is_gpu = self.current_job['is_gpu']
            current_timestamp = self.current_job['submit']

            # Display job details
            job_text = f"Job ID: {job_id}\nRequired Processors: {req_proc}\nGPU Job: {'Yes' if is_gpu == 1 else 'No'}"
            self.job_label.config(text=job_text)

            # Update utilization graph
            self.update_utilization_graph(current_timestamp)

            # Disable the "Next Job" button while user selects a cluster
            self.next_button.config(state=tk.DISABLED)

            # Disable cluster buttons if GPU job
            if is_gpu == 1:
                self.polaris_button.config(state=tk.DISABLED)
                self.theta_button.config(state=tk.DISABLED)
                messagebox.showinfo("GPU Job", "This job is a GPU job and will be automatically assigned to Polaris.")
                self.assign_job_to_cluster("Polaris")
            else:
                # Calculate turnaround times for non-GPU jobs
                turnarounds = {}
                scale_factors = [1.0, 4.0] if self.current_job['cluster_id'] == 0 else [0.25, 1.0]
                turnarounds = self.cqp.predict_next_job_turnarounds(
                    [self.polaris, self.theta],
                    job_id,
                    req_proc,
                    scale_factors
                )

                # Display turnaround times
                turnaround_text = (
                    f"Predicted Turnaround Times:\n"
                    f"Polaris: {turnarounds[0]} seconds\n"
                    f"Theta: {turnarounds[1]} seconds"
                )
                self.turnaround_label.config(text=turnaround_text)

                # Enable cluster buttons
                self.polaris_button.config(state=tk.NORMAL)
                self.theta_button.config(state=tk.NORMAL)

        except StopIteration:
            messagebox.showinfo("End of Jobs", "All jobs have been processed.")
            self.next_button.config(state=tk.DISABLED)

    def update_utilization_graph(self, current_timestamp):
        theta_utilization = self.theta_ult[
            (self.theta_ult['timestamp'] <= current_timestamp)
        ]
        print(theta_utilization)
        polaris_utilization = self.polaris_ult[
            (self.polaris_ult['timestamp'] <= current_timestamp)
        ]
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
        job_id = self.current_job['id']

        if self.current_job['cluster_id'] == 0:
            # Originally a polaris job
            cqp.set_job_run_scale_factor(polaris, 1.0)
            cqp.set_job_walltime_scale_factor(polaris, 1.0)
            cqp.set_job_run_scale_factor(theta, 4.0)
            cqp.set_job_walltime_scale_factor(theta, 4.0)
        elif self.current_job['cluster_id'] == 1:
            # Originally a theta job
            cqp.set_job_run_scale_factor(polaris, 0.25)
            cqp.set_job_walltime_scale_factor(polaris, 0.25)
            cqp.set_job_run_scale_factor(theta, 1.0)
            cqp.set_job_walltime_scale_factor(theta, 1.0)
        else:
            assert True == False

        if cluster_name == "Polaris":
            selected_sim = self.polaris
        elif cluster_name == "Theta":
            selected_sim = self.theta

        # Update the CqSim instance
        self.cqp.enable_next_job(selected_sim)
        for sim in [self.polaris, self.theta]:
            if sim == selected_sim:
                # Enable the next job in the mask
                self.cqp.enable_next_job(sim)
            else:
                cqp.disable_next_job(sim)
            with disable_print():
                self.cqp.line_step(sim, write_results=False)

        # Simulate the selected job
        messagebox.showinfo("Job Assigned", f"Job {job_id} assigned to {cluster_name}.")

        # Reset buttons and UI for the next job
        self.polaris_button.config(state=tk.DISABLED)
        self.theta_button.config(state=tk.DISABLED)
        self.turnaround_label.config(text="Turnaround Times")
        self.next_button.config(state=tk.NORMAL)


# Main Script
if __name__ == '__main__':
    # Setup the simulation environment
    exp_name = 'vis'
    trace_dir = '../preprocessing/output'
    trace_file = 'polaris_theta_23.swf'
    cqp = Cqsim_plus()
    cqp.set_exp_directory(f'../data/Results/exp_polaris_theta/vis')
    theta_ult_path = '../data/Results/exp_polaris_theta/vis/theta/Results/polaris_theta_23.ult'
    polaris_ult_path = '../data/Results/exp_polaris_theta/vis/polaris/Results/polaris_theta_23.ult'

    polaris_proc = 552
    theta_proc = 4360
    polaris = cqp.single_cqsim(trace_dir, trace_file, proc_count=polaris_proc, parsed_trace=False, sim_tag='polaris')
    theta = cqp.single_cqsim(trace_dir, trace_file, proc_count=theta_proc, parsed_trace=False, sim_tag='theta')

    read_job_gen = read_swf_generator(f'{trace_dir}/{trace_file}')
    first_job_submit_time = (next(read_job_gen))['submit']

    # Get the total number of jobs
    num_jobs = num_jobs_swf(f'{trace_dir}/{trace_file}')

    # Configure the simulators
    for sim in [polaris, theta]:
        # Read all jobs
        cqp.set_max_lines(sim, num_jobs)

        # Set the real time sart time and virtual start times
        cqp.set_sim_times(sim, real_start_time=first_job_submit_time, virtual_start_time=0)

        # Disbale the debug module
        cqp.disable_debug_module(sim)

    # Configure job generator
    read_job_gen = read_swf_generator(f'{trace_dir}/{trace_file}')
    theta_ult = read_ult(theta_ult_path)
    polaris_ult = read_ult(polaris_ult_path)

    # Start GUI
    root = tk.Tk()
    app = JobSchedulerGUI(root, read_job_gen, cqp, polaris, theta, polaris_ult, theta_ult)
    root.mainloop()