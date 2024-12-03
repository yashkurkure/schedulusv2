import os

def count_lines_of_code(directory):
  """
  Counts the total number of lines of code in all Python files within a given directory.

  Args:
    directory: The path to the directory to search.

  Returns:
    The total number of lines of code.
  """
  total_lines = 0
  for root, _, files in os.walk(directory):
    for file in files:
      if file.endswith(".py"):
        file_path = os.path.join(root, file)
        with open(file_path, "r") as f:  # Corrected line
          total_lines += sum(1 for line in f)
  return total_lines

# Example usage:
directory_to_search = "./src"
total_lines = count_lines_of_code(directory_to_search)
print(f"Total lines of code in directory '{directory_to_search}': {total_lines}")