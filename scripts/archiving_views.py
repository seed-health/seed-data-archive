import pandas as pd
import os

def read_csv_and_create_sql_files(csv_file_path, output_directory):

    # Read CSV file into a pandas DataFrame
    try:
        df = pd.read_csv(csv_file_path)
        print("CSV file successfully loaded into DataFrame.")
    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' was not found.")
        return
    except Exception as e:
        print(f"An error occurred: {e}")
        return

    # Access the first and second columns and iterate over their values
    first_column_index = 0  # Index 0 corresponds to the first column
    second_column_index = 1  # Index 1 corresponds to the second column
    first_column_values = df.iloc[:, first_column_index]
    second_column_values = df.iloc[:, second_column_index]

    print("\nCreating SQL files for values in the second column:")
    for index, (first_value, second_value) in enumerate(zip(first_column_values, second_column_values)):
        view_name = f'{first_value}'  # Customize the view name format here

        # Replace invalid characters in the file name
        view_name = ''.join(c if c.isalnum() or c in ('_', '-') else '_' for c in view_name)

        # Create the output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)

        # Create a file named after the view name and write the value into that file
        file_path = os.path.join(output_directory, f'{view_name}.sql')
        with open(file_path, 'w') as sql_file:
            sql_file.write(second_value)

        print(f"Created SQL file: {file_path}")

if __name__ == "__main__":
    # Replace 'your_file.csv' with the actual path to your CSV file
    csv_file_path = '/Users/salmaelmasry/Desktop/seed-data-archive/marketing_db.csv'
    
    # Specify the output directory
    output_directory = '/Users/salmaelmasry/Desktop/seed-data-archive/marketing_database/views'
    
    # Read CSV file and create SQL files for the first and second columns
    read_csv_and_create_sql_files(csv_file_path, output_directory)
