"""
Social Media Analytics MapReduce Workflow Driver

This script orchestrates the execution of the entire MapReduce workflow
for social media analytics using the local simulator.

Usage:
    python social_media_analytics_driver.py [--config CONFIG_FILE]
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("workflow.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SocialMediaAnalytics")


class LocalMapReduceWorkflow:
    """
    Orchestrates the execution of the social media analytics MapReduce workflow
    using the local simulator.
    """

    def __init__(self, config_file="config.json"):
        """
        Initialize the workflow with configuration settings.

        Args:
            config_file: Path to the configuration file (JSON format)
        """
        self.start_time = time.time()
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

        # Load configuration
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load configuration: {str(e)}")
            self.config = {
                "input_dir": "data",
                "output_dir": "output",
                "trending_threshold": "-1"
            }
            logger.warning("Using default configuration")

    def run_command(self, command):
        """
        Execute a shell command and log the output.

        Args:
            command: The shell command to execute

        Returns:
            The return code of the command
        """
        logger.info(f"Executing: {command}")
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )

        stdout, stderr = process.communicate()

        if stdout:
            logger.info(f"Command output: {stdout}")

        if stderr:
            if process.returncode != 0:
                logger.error(f"Command error: {stderr}")
            else:
                logger.warning(f"Command stderr: {stderr}")

        return process.returncode

    def run_data_cleansing(self):
        """Execute the data cleansing MapReduce job."""
        logger.info("Starting Data Cleansing job")

        simulator_script = os.path.join(self.base_dir, "local_mapreduce.py")
        command = f'python "{simulator_script}" --job cleansing --input-dir "{self.config["input_dir"]}" --output-dir "{self.config["output_dir"]}"'

        return self.run_command(command) == 0

    def run_action_aggregation(self):
        """Execute the action aggregation MapReduce job."""
        logger.info("Starting Action Aggregation job")

        simulator_script = os.path.join(self.base_dir, "local_mapreduce.py")
        command = f'python "{simulator_script}" --job aggregation --input-dir "{self.config["output_dir"]}" --output-dir "{self.config["output_dir"]}"'

        return self.run_command(command) == 0

    def run_trending_content(self):
        """Execute the trending content identification MapReduce job."""
        logger.info("Starting Trending Content Identification job")

        simulator_script = os.path.join(self.base_dir, "local_mapreduce.py")
        command = f'python "{simulator_script}" --job trending --input-dir "{self.config["output_dir"]}" --output-dir "{self.config["output_dir"]}"'

        return self.run_command(command) == 0

    def run_data_join(self):
        """Execute the dataset joining MapReduce job."""
        logger.info("Starting Data Join job")

        simulator_script = os.path.join(self.base_dir, "local_mapreduce.py")
        command = f'python "{simulator_script}" --job join --input-dir "{self.config["output_dir"]}" --output-dir "{self.config["output_dir"]}"'

        return self.run_command(command) == 0

    def run_full_workflow(self):
        """
        Execute the complete MapReduce workflow from data cleansing to joining.
        """
        logger.info("Starting complete social media analytics workflow")

        simulator_script = os.path.join(self.base_dir, "local_mapreduce.py")
        command = f'python "{simulator_script}" --job all --input-dir "{self.config["input_dir"]}" --output-dir "{self.config["output_dir"]}"'

        success = self.run_command(command) == 0

        # Calculate and log total execution time
        execution_time = time.time() - self.start_time
        if success:
            logger.info(f"Complete workflow executed successfully in {execution_time:.2f} seconds")
            self.display_workflow_stats()
        else:
            logger.error(f"Workflow failed after {execution_time:.2f} seconds")

        return success

    def display_workflow_stats(self):
        """
        Collect and display final workflow statistics.
        """
        logger.info("Collecting workflow statistics")

        # Get record counts from each output file
        try:
            cleansed_path = os.path.join(self.config["output_dir"], "cleansed_data.txt")
            activity_path = os.path.join(self.config["output_dir"], "user_activity.txt")
            trending_path = os.path.join(self.config["output_dir"], "trending_content.txt")
            joined_path = os.path.join(self.config["output_dir"], "joined_data.txt")

            cleansed_count = sum(1 for _ in open(cleansed_path, 'r', encoding='utf-8'))
            user_activity_count = sum(1 for _ in open(activity_path, 'r', encoding='utf-8'))
            trending_count = sum(1 for _ in open(trending_path, 'r', encoding='utf-8'))
            joined_count = sum(1 for _ in open(joined_path, 'r', encoding='utf-8'))

            stats = {
                "Execution Time (s)": round(time.time() - self.start_time, 2),
                "Cleansed Records": cleansed_count,
                "Unique Users": user_activity_count,
                "Trending Content Items": trending_count,
                "Joined User Records": joined_count
            }

            # Create a summary report
            summary = "\n" + "=" * 50 + "\n"
            summary += "SOCIAL MEDIA ANALYTICS WORKFLOW SUMMARY\n"
            summary += "=" * 50 + "\n"

            for stat, value in stats.items():
                summary += f"{stat}: {value}\n"

            summary += "=" * 50

            logger.info(summary)

            # Save to a summary file
            with open(os.path.join(self.config["output_dir"], "workflow_summary.txt"), "w") as f:
                f.write(summary)

        except Exception as e:
            logger.error(f"Error collecting workflow statistics: {str(e)}")


def main():
    """Main entry point for the workflow driver."""
    parser = argparse.ArgumentParser(description="Social Media Analytics MapReduce Workflow")
    parser.add_argument('--config', default='config.json', help='Configuration file path')
    args = parser.parse_args()

    try:
        # Initialize and run the workflow
        workflow = LocalMapReduceWorkflow(args.config)
        success = workflow.run_full_workflow()

        sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Workflow execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()