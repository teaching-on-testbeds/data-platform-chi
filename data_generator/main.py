#!/usr/bin/env python3
import asyncio
import logging
import os
import random
import signal
import sys
from pathlib import Path
from typing import Optional

import numpy as np

from .event_generators import GourmetGramEventGenerators

# Config for data generator
class Config:
    def __init__(self):
        # API Configuration
        self.api_url = os.getenv("GENERATOR_API_URL", "http://localhost:8000")

        # Dataset Configuration
        self.dataset_path = os.getenv("GENERATOR_DATASET_PATH", "/data/Food-11")

        # Traffic Generation
        self.arrival_rate = float(os.getenv("GENERATOR_ARRIVAL_RATE", "150.0"))

        # Bootstrap Configuration
        self.initial_users = int(os.getenv("GENERATOR_INITIAL_USERS", "0"))
        self.initial_images = int(os.getenv("GENERATOR_INITIAL_IMAGES", "0"))

        # HTTP Configuration
        self.request_timeout = float(os.getenv("GENERATOR_REQUEST_TIMEOUT", "30.0"))

        # Logging Configuration
        self.log_level = os.getenv("GENERATOR_LOG_LEVEL", "INFO")

        # Mode Configuration
        self.mode = os.getenv("GENERATOR_MODE", "poisson")
        self.target_image_id = os.getenv("GENERATOR_TARGET_IMAGE_ID")
        self.burst_views = int(os.getenv("GENERATOR_BURST_VIEWS", "110"))
        self.burst_comments = int(os.getenv("GENERATOR_BURST_COMMENTS", "8"))
        self.burst_flags = int(os.getenv("GENERATOR_BURST_FLAGS", "2"))



# EVENT_PROBABILITIES = {
#     "view": 0.68,      
#     "comment": 0.20,   
#     "upload": 0.07,    
#     "user": 0.03,      
#     "flag": 0.02       
# }

class PoissonEventGenerator:

    def __init__(self, mean_rate: float, event_callback, name: str = "Event"):

        self.mean_rate = mean_rate
        self.event_callback = event_callback
        self.name = name
        self.running = False
        self.event_count = 0

        # Convert rate from events/hour to events/second
        self.rate_per_second = mean_rate / 3600.0

    async def start(self):
        self.running = True
        logger = logging.getLogger(__name__)
        logger.info(f"Started {self.name} generator")

        while self.running:
            # Generate inter-arrival time from exponential distribution
            wait_time = np.random.exponential(1.0 / self.rate_per_second)

            # Wait for the randomly generated time
            await asyncio.sleep(wait_time)

            # Execute the event callback
            if self.running:
                try:
                    await self.event_callback()
                    self.event_count += 1

                    if self.event_count % 10 == 0:
                        logger.debug(
                            f"{self.name}: Generated {self.event_count} events "
                            f"(rate: {self.mean_rate:.1f}/hr)"
                        )

                except Exception as e:
                    logger.error(f"{self.name} event failed: {e}", exc_info=True)
                    await asyncio.sleep(1.0)

    def stop(self):
        self.running = False
        logger = logging.getLogger(__name__)
        logger.info(
            f"Stopped {self.name} generator "
            f"(generated {self.event_count} total events)"
        )

def setup_logging(log_level: str):
    level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Reduce noise from httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

async def main():
    # Load configuration
    config = Config()
    setup_logging(config.log_level)
    logger = logging.getLogger(__name__)

    logger.info("=" * 70)
    logger.info("GOURMETGRAM DATA GENERATOR")
    logger.info("=" * 70)
    logger.info(f"API URL: {config.api_url}")
    logger.info(f"Dataset Path: {config.dataset_path}")
    logger.info(f"Arrival Rate: {config.arrival_rate:.2f} events/hour")
    logger.info(f"Mode: {config.mode}")
    logger.info(f"Log Level: {config.log_level}")
    logger.info("=" * 70)

    # Initialize event generators
    generators = GourmetGramEventGenerators(
        api_base_url=config.api_url,
        dataset_path=Path(config.dataset_path),
        timeout=config.request_timeout
    )

    # Bootstrap: Create initial users and images if configured
    if config.initial_users > 0:
        logger.info(f"Bootstrapping {config.initial_users} initial users...")
        for i in range(config.initial_users):
            await generators.generate_user()
            if (i + 1) % 5 == 0:
                logger.info(f"  Created {i + 1}/{config.initial_users} users")

    if config.initial_images > 0:
        logger.info(f"Bootstrapping {config.initial_images} initial images...")
        for i in range(config.initial_images):
            await generators.generate_upload()
            if (i + 1) % 5 == 0:
                logger.info(f"  Uploaded {i + 1}/{config.initial_images} images")

    if config.mode == "simulate_viral":
        logger.info("")
        logger.info("=" * 70)
        logger.info("SIMULATE VIRAL MODE")
        logger.info("=" * 70)

        if not generators.users:
            await generators.generate_user()

        target_image_id = config.target_image_id
        if not target_image_id:
            if not generators.images:
                uploaded = await generators.generate_upload()
                if uploaded:
                    target_image_id = uploaded["id"]
            else:
                target_image_id = random.choice(generators.images)

        if not target_image_id:
            logger.error("Could not determine target image for viral simulation")
            await generators.close()
            return

        logger.info(f"Simulating viral burst for image: {target_image_id}")
        logger.info(
            f"Burst plan: views={config.burst_views}, comments={config.burst_comments}, flags={config.burst_flags}"
        )

        for _ in range(config.burst_views):
            await generators.generate_view_for_image(target_image_id)

        for _ in range(config.burst_comments):
            await generators.generate_comment_for_image(target_image_id)

        for _ in range(config.burst_flags):
            await generators.generate_flag_for_image(target_image_id)

        logger.info("Viral simulation complete")
        generators.print_stats()
        await generators.close()
        return

    # Display expected rates
    logger.info("")
    logger.info("=" * 70)
    logger.info("STARTING POISSON GENERATOR")
    logger.info("=" * 70)
    logger.info(f"Total arrival rate: {config.arrival_rate:.2f} events/hour")
    logger.info("")
    logger.info("Expected event distribution:")
    # for event_type, prob in EVENT_PROBABILITIES.items():
    #     expected_rate = config.arrival_rate * prob
    #     logger.info(f"  {event_type.capitalize():10s} {expected_rate:6.2f} events/hour ({prob:.0%})")
    logger.info(f"Event distribution: Equal (20% each)")
    logger.info("=" * 70)
    logger.info("")

    # Create callback that randomly selects event type
    async def random_event_callback():
        # event_types = list(EVENT_PROBABILITIES.keys())
        # probabilities = list(EVENT_PROBABILITIES.values())

        # Randomly select event type
        # event_type = random.choices(event_types, weights=probabilities, k=1)[0]
        event_type = random.choice(['view', 'comment', 'upload', 'user', 'flag'])

        # Execute the corresponding generator
        if event_type == "view":
            await generators.generate_view()
        elif event_type == "comment":
            await generators.generate_comment()
        elif event_type == "upload":
            await generators.generate_upload()
        elif event_type == "user":
            await generators.generate_user()
        elif event_type == "flag":
            await generators.generate_flag()

    # Create single Poisson generator
    generator = PoissonEventGenerator(
        mean_rate=config.arrival_rate,
        event_callback=random_event_callback,
        name="Traffic Generator"
    )

    # Setup graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"\nReceived signal {sig}, initiating graceful shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start periodic stats reporting
    async def print_stats_periodically():
        while not shutdown_event.is_set():
            await asyncio.sleep(300)  # 5 minutes
            if not shutdown_event.is_set():
                generators.print_stats()

    stats_task = asyncio.create_task(print_stats_periodically())

    # Run generator
    try:
        logger.info("Generator is running! Press Ctrl+C to stop.")
        logger.info("")

        # Start generator
        generator_task = asyncio.create_task(generator.start())

        # Wait for shutdown signal
        await shutdown_event.wait()

        # Stop generator
        logger.info("Stopping generator...")
        generator.stop()

        # Cancel tasks
        stats_task.cancel()
        generator_task.cancel()

        # Wait for tasks to finish with timeout
        await asyncio.wait(
            [generator_task, stats_task],
            timeout=5.0
        )

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

    finally:
        # Print final statistics
        logger.info("")
        logger.info("FINAL STATISTICS")
        generators.print_stats()

        # Cleanup
        await generators.close()
        logger.info("Data generator shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
        sys.exit(0)
