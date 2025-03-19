import os
from dotenv import load_dotenv

def get_filenames(directory, extension):
    # Ensure the extension starts with a dot
    if not extension.startswith('.'):
        extension = '.' + extension

    # Get all files with the given extension
    files = [f for f in os.listdir(directory) if f.endswith(extension)]

    # Remove the extension from each filename and wrap in double quotes
    filenames = [f'"{os.path.splitext(f)[0]}"' for f in files]

    # Join the filenames with commas
    return ','.join(filenames)

load_dotenv()

directory =  os.getenv('DIRECTORY')
extension =  os.getenv('EXTENSION')

result = get_filenames(directory, extension)
print(result)
