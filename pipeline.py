import luigi
import requests
import tarfile
import os
import gzip
import shutil
import subprocess
import logging
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DownloadFile(luigi.Task):
    """
    Task to download a file from a given URL.
    """
    url = luigi.Parameter()
    output_dir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'archive.tar'))

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        response = requests.get(self.url, stream=True)
        total_size = int(response.headers.get('content-length', 0))

        with open(self.output().path, 'wb') as f:
            for data in tqdm(response.iter_content(1024), total=total_size // 1024, desc="Downloading"):
                f.write(data)
        logging.info(f"File downloaded to {self.output().path}")


class VerifyAndExtractTar(luigi.Task):
    """
    Task to verify and extract a TAR file.
    """
    url = luigi.Parameter()
    output_dir = luigi.Parameter()

    def requires(self):
        return DownloadFile(self.url, self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'extracted'))

    def run(self):
        tar_file_path = self.input().path
        extract_dir = self.output().path
        os.makedirs(extract_dir, exist_ok=True)

        try:
            with tarfile.open(tar_file_path, 'r') as tar_ref:
                tar_ref.getmembers()
            logging.info(f"TAR file {tar_file_path} passed integrity check.")
        except Exception as e:
            raise Exception(f"TAR file integrity check failed: {e}")

        with tarfile.open(tar_file_path, 'r') as tar_ref:
            tar_ref.extractall(path=extract_dir)
        logging.info(f"Extracted files to {extract_dir}")


class OrganizeAndProcessFiles(luigi.Task):
    """
    Task to organize and process extracted files.
    """
    url = luigi.Parameter()
    output_dir = luigi.Parameter()
    delete_archives = luigi.BoolParameter(default=True)

    def requires(self):
        return VerifyAndExtractTar(self.url, self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'processed'))

    def run(self):
        extracted_dir = self.input().path
        processed_dir = self.output().path
        os.makedirs(processed_dir, exist_ok=True)

        for file_name in os.listdir(extracted_dir):
            file_path = os.path.join(extracted_dir, file_name)
            base_name, ext = os.path.splitext(file_name)

            file_folder = os.path.join(processed_dir, base_name)
            os.makedirs(file_folder, exist_ok=True)

            if ext == ".gz":
                decompressed_file = os.path.join(file_folder, base_name)
                with gzip.open(file_path, 'rb') as f_in:
                    with open(decompressed_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                logging.info(f"Decompressed {file_name} to {decompressed_file}")
            else:
                shutil.copy(file_path, file_folder)
                logging.info(f"Copied {file_name} to {file_folder}")

        if self.delete_archives:
            shutil.rmtree(extracted_dir)
            logging.info(f"Deleted original extracted folder {extracted_dir}")

        logging.info(f"Organized files into {processed_dir}")


class RunExternalScript(luigi.Task):
    """
    Task to run an external script on processed files.
    """
    url = luigi.Parameter()
    output_dir = luigi.Parameter()
    script_path = luigi.Parameter(default="toTabs.py")

    def requires(self):
        return OrganizeAndProcessFiles(self.url, self.output_dir)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'completed'))

    def run(self):
        processed_dir = self.input().path
        os.makedirs(self.output().path, exist_ok=True)

        for folder in os.listdir(processed_dir):
            folder_path = os.path.join(processed_dir, folder)
            file_path = os.path.join(folder_path, folder)  # Assuming one file per folder

            if os.path.isfile(file_path):
                try:
                    subprocess.run(['python3.9', self.script_path, file_path], check=True)
                    logging.info(f"Processed {file_path} with {self.script_path}")
                except subprocess.CalledProcessError as e:
                    logging.error(f"Error processing {file_path}: {e}")
            else:
                logging.warning(f"Skipped {folder_path}: Not a file.")

        logging.info(f"All files processed. Results stored in {processed_dir}")

if __name__ == "__main__":
    luigi.run()
