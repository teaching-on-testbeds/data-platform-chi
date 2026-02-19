import httpx
import random
import logging
import string
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional
from faker import Faker

logger = logging.getLogger(__name__)


# Food-11 dataset class mapping (class_00 to class_10)
FOOD11_CATEGORIES = [
    "Bread",           # class_00
    "Dairy product",   # class_01
    "Dessert",         # class_02
    "Egg",             # class_03
    "Fried food",      # class_04
    "Meat",            # class_05
    "Noodles/Pasta",   # class_06
    "Rice",            # class_07
    "Seafood",         # class_08
    "Soup",            # class_09
    "Vegetable/Fruit"  # class_10
]

COUNTRY_CODES = [
    "US", "IN", "BR", "DE", "GB", "CA", "MX", "FR", "JP", "KR", "NG", "ZA"
]

CAPTION_OPENERS = [
    "Tonight we made",
    "Trying out",
    "Weekend special",
    "Homemade",
    "Quick lunch",
    "Family dinner with",
    "Testing this",
    "Fresh from the kitchen",
]

COMMENT_PREFIXES = [
    "Looks",
    "This is",
    "Wow,",
    "Honestly",
    "I think this",
    "Can we get",
    "I would",
    "Great",
]

FLAG_REASONS = [
    "harassment in caption",
    "spam in comment",
    "offensive language",
    "violent content",
    "hate speech",
    "misleading content",
    "graphic description",
    "personal attack",
]


def random_string(length: int = 10) -> str:
    return ''.join(random.choices(string.ascii_letters, k=length))


class GourmetGramEventGenerators:

    def __init__(self, api_base_url: str, dataset_path: Path, timeout: float = 30.0):
        self.api_url = api_base_url
        self.dataset_path = Path(dataset_path)
        self.client = httpx.AsyncClient(timeout=timeout)
        self.faker = Faker()
        self.tos_version_accepted = int(os.getenv("GENERATOR_TOS_VERSION", "3"))
        self.test_account_rate = float(os.getenv("GENERATOR_TEST_ACCOUNT_RATE", "0.02"))

        # State management - track created entities for causality
        self.users: List[str] = []
        self.images: List[str] = []
        self.comments: List[str] = []
        self.image_engagement: Dict[str, int] = {}
        self.comment_to_image: Dict[str, str] = {}

        # Counters for statistics
        self.stats = {
            "users": 0,
            "uploads": 0,
            "views": 0,
            "comments": 0,
            "flags": 0,
            "errors": 0
        }

        logger.info(f"Initialized event generators (API: {api_base_url})")

        if not self.dataset_path.exists():
            logger.warning(f"Dataset path does not exist: {self.dataset_path}")
        else:
            logger.info(f"Dataset path: {self.dataset_path}")

    async def generate_user(self) -> Optional[Dict]:
        try:
            username = self._generate_username()
            now = datetime.now(timezone.utc)
            user_payload = {
                "username": username,
                "tos_version_accepted": self.tos_version_accepted,
                "tos_accepted_at": now.isoformat(),
                "country_of_residence": random.choice(COUNTRY_CODES),
                "year_of_birth": random.randint(1960, 2007),
                "is_test_account": random.random() < self.test_account_rate,
            }

            response = await self.client.post(
                f"{self.api_url}/users/",
                json=user_payload
            )
            response.raise_for_status()

            user_data = response.json()
            user_id = user_data["id"]

            self.users.append(user_id)
            self.stats["users"] += 1

            logger.info(f"Created user: {username} ({user_id})")
            return user_data

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to create user: {e}")
            return None

    async def generate_upload(self) -> Optional[Dict]:
        if not self.users:
            logger.warning("No users available - creating one first")
            await self.generate_user()
            if not self.users:
                logger.error("Failed to create user - cannot upload image")
                return None

        try:
            image_path = self._get_random_image()
            if not image_path:
                logger.error("No images found in dataset")
                return None

            category = self._extract_category(image_path)
            caption = self._generate_caption(category)
            user_id = random.choice(self.users)

            with open(image_path, 'rb') as f:
                files = {'file': (image_path.name, f, 'image/jpeg')}
                data = {
                    'user_id': user_id,
                    'caption': caption,
                    'category': category
                }

                response = await self.client.post(
                    f"{self.api_url}/upload/",
                    files=files,
                    data=data
                )
                response.raise_for_status()

            image_data = response.json()
            image_id = image_data["id"]

            self.images.append(image_id)
            self.image_engagement[image_id] = 1
            self.stats["uploads"] += 1

            logger.info(
                f"Uploaded image: {category} by user {user_id[:8]}... "
                f"(id: {image_id[:8]}...)"
            )
            return image_data

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to upload image: {e}")
            return None

    async def generate_view(self) -> bool:
        if not self.images:
            logger.debug("No images available to view - skipping")
            return False
        image_id = self._choose_image_non_uniform()
        return await self.generate_view_for_image(image_id)

    async def generate_view_for_image(self, image_id: str) -> bool:
        try:
            response = await self.client.post(f"{self.api_url}/images/{image_id}/view")
            response.raise_for_status()
            result = response.json()
            view_count = result.get("views", "?")
            self.stats["views"] += 1
            self.image_engagement[image_id] = self.image_engagement.get(image_id, 1) + 1
            logger.debug(f"Recorded view on image {image_id[:8]}... (total: {view_count})")
            return True
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to record view for image {image_id[:8]}...: {e}")
            return False

    async def generate_comment(self) -> Optional[Dict]:
        if not self.images:
            logger.debug("No images available to comment on - skipping")
            return None
        image_id = self._choose_image_non_uniform()
        return await self.generate_comment_for_image(image_id)

    async def generate_comment_for_image(self, image_id: str) -> Optional[Dict]:
        if not self.users:
            logger.warning("No users available - creating one first")
            await self.generate_user()
            if not self.users:
                return None

        try:
            user_id = random.choice(self.users)
            content = self._generate_comment()

            response = await self.client.post(
                f"{self.api_url}/comments/",
                json={
                    "image_id": image_id,
                    "user_id": user_id,
                    "content": content,
                },
            )
            response.raise_for_status()

            comment_data = response.json()
            comment_id = comment_data["id"]

            self.comments.append(comment_id)
            self.comment_to_image[comment_id] = image_id
            self.image_engagement[image_id] = self.image_engagement.get(image_id, 1) + 2
            self.stats["comments"] += 1

            logger.info(
                f"Added comment on image {image_id[:8]}... "
                f"by user {user_id[:8]}... (id: {comment_id[:8]}...)"
            )
            return comment_data

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to add comment for image {image_id[:8]}...: {e}")
            return None

    async def generate_flag(self) -> Optional[Dict]:
        if not self.users:
            logger.warning("No users available - creating one first")
            await self.generate_user()
            if not self.users:
                return None

        can_flag_image = len(self.images) > 0
        can_flag_comment = len(self.comments) > 0

        if not can_flag_image and not can_flag_comment:
            logger.debug("No images or comments to flag - skipping")
            return None

        try:
            if can_flag_image and can_flag_comment:
                flag_target = random.choices(["image", "comment"], weights=[0.5, 0.5])[0]
            elif can_flag_image:
                flag_target = "image"
            else:
                flag_target = "comment"

            user_id = random.choice(self.users)
            payload = {"user_id": user_id, "reason": self._generate_flag_reason()}

            if flag_target == "image":
                target_id = self._choose_image_non_uniform()
                payload["image_id"] = target_id
                target_type = "image"
            else:
                target_id = random.choice(self.comments)
                payload["comment_id"] = target_id
                target_type = "comment"

            response = await self.client.post(f"{self.api_url}/flags/", json=payload)
            response.raise_for_status()

            flag_data = response.json()
            self.stats["flags"] += 1

            if target_type == "image":
                self.image_engagement[target_id] = self.image_engagement.get(target_id, 1) + 3
            else:
                linked_image_id = self.comment_to_image.get(target_id)
                if linked_image_id:
                    self.image_engagement[linked_image_id] = self.image_engagement.get(linked_image_id, 1) + 2

            logger.info(
                f"Created flag for {target_type} {target_id[:8]}... "
                f"by user {user_id[:8]}... (reason: {payload['reason']})"
            )
            return flag_data

        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to create flag: {e}")
            return None

    async def generate_flag_for_image(self, image_id: str) -> Optional[Dict]:
        if not self.users:
            await self.generate_user()
            if not self.users:
                return None

        try:
            user_id = random.choice(self.users)
            payload = {
                "user_id": user_id,
                "image_id": image_id,
                "reason": self._generate_flag_reason(),
            }
            response = await self.client.post(f"{self.api_url}/flags/", json=payload)
            response.raise_for_status()
            self.stats["flags"] += 1
            self.image_engagement[image_id] = self.image_engagement.get(image_id, 1) + 3
            return response.json()
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to create flag for image {image_id[:8]}...: {e}")
            return None

    def _get_random_image(self) -> Optional[Path]:
        try:
            splits = ['training', 'validation', 'evaluation']
            available_splits = [
                s for s in splits
                if (self.dataset_path / s).exists()
            ]

            if not available_splits:
                logger.error(f"No splits found in {self.dataset_path}")
                return None

            split = random.choice(available_splits)
            split_path = self.dataset_path / split

            image_extensions = {'.jpg', '.jpeg', '.png', '.JPG', '.JPEG', '.PNG'}
            images = [
                f for f in split_path.rglob('*')
                if f.is_file() and f.suffix in image_extensions
            ]

            if not images:
                logger.error(f"No images found in {split_path}")
                return None

            return random.choice(images)

        except Exception as e:
            logger.error(f"Error selecting random image: {e}")
            return None

    def _extract_category(self, image_path: Path) -> str:
        try:
            class_dir = image_path.parent.name

            if class_dir.startswith("class_"):
                class_idx = int(class_dir.split("_")[1])

                if 0 <= class_idx < len(FOOD11_CATEGORIES):
                    return FOOD11_CATEGORIES[class_idx]

            logger.warning(f"Could not extract category from {image_path}")
            return "Unknown"

        except Exception as e:
            logger.error(f"Error extracting category: {e}")
            return "Unknown"

    def _generate_caption(self, category: str) -> str:
        opener = random.choice(CAPTION_OPENERS)
        descriptor = self.faker.word(ext_word_list=["fresh", "crispy", "spicy", "sweet", "savory", "creamy"])
        tail = self.faker.sentence(nb_words=random.randint(3, 7)).rstrip(".")
        return f"{opener} {descriptor} {category.lower()} - {tail}."

    def _generate_comment(self) -> str:
        prefix = random.choice(COMMENT_PREFIXES)
        body = self.faker.sentence(nb_words=random.randint(4, 9)).rstrip(".")
        return f"{prefix} {body}."

    def _generate_flag_reason(self) -> str:
        return random.choice(FLAG_REASONS)

    def _generate_username(self) -> str:
        base = self.faker.user_name()
        suffix = random.randint(10, 9999)
        return f"{base}_{suffix}"

    def _choose_image_non_uniform(self) -> str:
        if not self.images:
            raise ValueError("No images available")

        # Prefer images with higher prior activity using weighted sampling.
        # This produces a non-uniform distribution (popular images are sampled more often).
        weighted_images = []
        weights = []
        for image_id in self.images:
            weighted_images.append(image_id)
            weights.append(float(self.image_engagement.get(image_id, 1)))

        return random.choices(weighted_images, weights=weights, k=1)[0]

    def print_stats(self):
        logger.info("=" * 50)
        logger.info("EVENT GENERATOR STATISTICS")
        logger.info("=" * 50)
        logger.info(f"Users created:     {self.stats['users']}")
        logger.info(f"Images uploaded:   {self.stats['uploads']}")
        logger.info(f"Views recorded:    {self.stats['views']}")
        logger.info(f"Comments added:    {self.stats['comments']}")
        logger.info(f"Flags created:     {self.stats['flags']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 50)
        logger.info(f"Current state: {len(self.users)} users, "
                   f"{len(self.images)} images, {len(self.comments)} comments")
        logger.info("=" * 50)

    async def close(self):
        await self.client.aclose()
        logger.info("Closed HTTP client")
