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

    # Access the second column and iterate over its values
    second_column_index = 1  # Index 1 corresponds to the second column
    second_column_values = df.iloc[:, second_column_index]

    print("\nCreating SQL files for values in the second column:")
    for index, value in enumerate(second_column_values):
        view_name = f'view_{index + 1}'  # You can customize the view name here

        # Create the output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)

        # Create a file named after the view name and write the value into that file
        file_path = os.path.join(output_directory, f'{view_name}.sql')
        with open(file_path, 'w') as sql_file:
            sql_file.write(value)

        print(f"Created SQL file: {file_path}")

if __name__ == "__main__":
    # Replace 'your_file.csv' with the actual path to your CSV file
    csv_file_path = '/Users/salmaelmasry/Desktop/marketing_db.csv'
    
    # Specify the output directory
    output_directory = '/Users/salmaelmasry/Desktop/seed-data-archive/marketing_database/views'
    
    # Read CSV file and create SQL files for the second column
    read_csv_and_create_sql_files(csv_file_path, output_directory)
