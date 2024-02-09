import requests
import json
import os
from typing import List, Dict, Any

class GitHubDataProcessor:
    """
    A class to scrape and process data from a specific GitHub repository using the GitHub API.
    This class uses a GitHub Personal Access Token for API authentication and a repository path
    to fetch and process relevant data from the repository.

    Attributes:
        token (str): GitHub Personal Access Token for API authentication.
        query (str): Path of the GitHub repository.
        headers (Dict[str, str]): Headers to use in the API request.

    Methods:
        fetch_data: Fetches data including all files from the specified repository.
        process_data: Processes the fetched data for LLM.
    """

    def __init__(self, token: str, query: str) -> None:
        """
        Initializes the GitHubDataProcessor with a token and a repository path.
        Args:
            token (str): GitHub Personal Access Token.
            query (str): Path of the GitHub repository.
        """
        self.token = token
        self.query = query
        self.headers = {'Authorization': f'token {token}'}

    def fetch_file_content(self, download_url: str) -> str:
        """
        Fetches the content of a single file from the GitHub repository.
        Args:
            download_url (str): The download URL of the file content.
        Returns:
            str: The content of the file.
        """
        try:
            response = requests.get(download_url, headers=self.headers)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            raise Exception(f"Error fetching file content from GitHub: {e}")

    def fetch_data(self) -> List[Dict[str, Any]]:
        """
        Fetches data including the contents of all files from the specified GitHub repository.
        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing file data and contents from the repository.
        """
        try:
            url = f"https://api.github.com/repos/{self.query}/contents"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            repo_contents = json.loads(response.text)
            detailed_contents = []

            for item in repo_contents:
                file_data = {
                    'name': item['name'],
                    'path': item['path'],
                    'url': item['html_url']
                }
                if item['type'] == 'file':
                    file_data['content'] = self.fetch_file_content(item['download_url'])
                detailed_contents.append(file_data)

            return detailed_contents
        except requests.RequestException as e:
            raise Exception(f"Error fetching data from GitHub: {e}")
    def fetch_directory_list(self) -> List[str]:
        """
        Fetches a list of directories in the specified GitHub repository.

        Returns:
            List[str]: A list of directory names in the repository.
        """
        try:
            url = f"https://api.github.com/repos/{self.query}/contents"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            contents = json.loads(response.text)

            directories = [item['name'] for item in contents if item['type'] == 'dir']
            return directories
        except requests.RequestException as e:
            raise Exception(f"Error fetching directory list from GitHub: {e}")

    def fetch_directory_data(self, directory_name: str) -> List[Dict[str, Any]]:
        """
        Fetches data including the contents of all files from a specified directory in the GitHub repository.

        Args:
            directory_name (str): The name of the directory to fetch data from.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing file data and contents from the specified directory.
        """
        try:
            url = f"https://api.github.com/repos/{self.query}/contents/{directory_name}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            directory_contents = json.loads(response.text)
            detailed_contents = []

            for item in directory_contents:
                file_data = {
                    'name': item['name'],
                    'path': item['path'],
                    'url': item['html_url']
                }
                if item['type'] == 'file':
                    file_data['content'] = self.fetch_file_content(item['download_url'])
                detailed_contents.append(file_data)

            return detailed_contents
        except requests.RequestException as e:
            raise Exception(f"Error fetching data from directory '{directory_name}' in GitHub: {e}")

    def process_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes the raw data fetched from GitHub to make it more digestible for LLM.
        Args:
            raw_data (List[Dict[str, Any]]): Raw data fetched from GitHub.
        Returns:
            List[Dict[str, Any]]: Processed data.
        """
        processed_data = []
        for item in raw_data:
            if 'content' in item:
                optimized_entry = self.optimize_entry(item)
                processed_data.append(optimized_entry)
        return processed_data

    def determine_file_type(file_name: str) -> str:
        """
        Determines the type of the given file based on its extension.

        Args:
            file_name (str): The name of the file.

        Returns:
            str: The determined file type.
        """
        _, ext = os.path.splitext(file_name)
        if ext in ['.py', '.java', '.cpp']:
            return 'Code'
        elif ext in ['.md', '.txt']:
            return 'Text'
        elif ext in ['.jpg', '.png', '.gif']:
            return 'Image'
        else:
            return 'Unknown'

    def summarize_file_content(file_path: str) -> str:
        """
        Summarizes the content of the given file by returning the first few lines.

        Args:
            file_path (str): The path to the file.

        Returns:
            str: A summary of the file content.
        """
        try:
            with open(file_path, 'r') as file:
                lines = file.readlines()
                return ''.join(lines[:5])  # Returning first 5 lines as a summary
        except Exception as e:
            return f"Error reading file: {e}"

    def extract_key_info(file_path: str) -> str:
        """
        Extracts key information from the given file. For demonstration, extracts version numbers.

        Args:
            file_path (str): The path to the file.

        Returns:
            str: Extracted key information from the file.
        """
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    if "version" in line.lower():
                        return line.strip()  # Return the line containing 'version'
                return "No version info found"
        except Exception as e:
            return f"Error reading file: {e}"


# Example Usage:
# token = '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'
# query = "NVIDIA/TensorRT"
# scraper = scraperModule.GitHubDataScraper(token=token,query=query)
# raw_data = scraper.fetch_data()
# cleaned_data = scraper.clean_data(raw_data)
# print(cleaned_data)

#processor = OptimizedDataProcessor(cleaned_data)
#optimized_data = processor.process_data()