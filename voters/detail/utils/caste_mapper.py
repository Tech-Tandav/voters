"""
Caste Mapping Utility

Maps Nepali surnames to caste/ethnic groups.
Uses the SurnameMapping model from database.
"""

from django.core.cache import cache
from voters.models import SurnameMapping
import logging

logger = logging.getLogger(__name__)


class CasteMapper:
    """
    Maps surnames to caste groups using database mappings.
    Implements caching for better performance.
    """
    
    CACHE_KEY = 'surname_caste_mapping'
    CACHE_TIMEOUT = 3600  # 1 hour
    
    def __init__(self):
        """Initialize the mapper and load mappings from database."""
        self.mappings = self._load_mappings()
    
    def _load_mappings(self):
        """
        Load surname mappings from database with caching.
        
        Returns:
            dict: Dictionary of {surname: caste_group}
        """
        # Try to get from cache first
        cached_mappings = cache.get(self.CACHE_KEY)
        if cached_mappings:
            logger.debug("Loaded surname mappings from cache")
            return cached_mappings
        
        # Load from database
        try:
            mappings = {}
            for mapping in SurnameMapping.objects.filter(is_active=True):
                mappings[mapping.surname] = mapping.caste_group
            
            # Cache the mappings
            cache.set(self.CACHE_KEY, mappings, self.CACHE_TIMEOUT)
            logger.info(f"Loaded {len(mappings)} surname mappings from database")
            
            return mappings
        
        except Exception as e:
            logger.error(f"Error loading surname mappings: {e}")
            return {}
    
    def get_caste_group(self, surname):
        """
        Get caste group for a given surname.
        
        Args:
            surname (str): Surname to lookup
        
        Returns:
            str: Caste group ('brahmin', 'chhetri', etc.) or 'unknown'
        """
        if not surname:
            return 'unknown'
        
        # Direct lookup
        caste_group = self.mappings.get(surname)
        
        if caste_group:
            return caste_group
        
        # Try case-insensitive search for English names
        for key, value in self.mappings.items():
            if key == surname:
                return value
            # print(key)
            # print(surname)
            # print("***********")
        # Not found
        logger.debug(f"Surname not mapped: {surname}")
        return 'unknown'
    
    def reload(self):
        """
        Reload mappings from database.
        Useful after updating surname mappings in admin.
        """
        cache.delete(self.CACHE_KEY)
        self.mappings = self._load_mappings()
        logger.info("Surname mappings reloaded")
    
    def get_unmapped_surnames(self, surnames):
        """
        Get list of surnames that are not in mapping.
        Useful for identifying missing mappings.
        
        Args:
            surnames (list): List of surnames to check
        
        Returns:
            list: Surnames not found in mapping
        """
        unmapped = []
        for surname in surnames:
            if self.get_caste_group(surname) == 'unknown':
                unmapped.append(surname)
        return unmapped
    
    def add_mapping(self, surname, caste_group):
        """
        Add a new surname mapping to database.
        
        Args:
            surname (str): Surname
            caste_group (str): Caste group
        
        Returns:
            bool: Success status
        """
        try:
            SurnameMapping.objects.update_or_create(
                surname=surname,
                defaults={'caste_group': caste_group, 'is_active': True}
            )
            self.reload()  # Reload cache
            return True
        except Exception as e:
            logger.error(f"Error adding surname mapping: {e}")
            return False


# Singleton instance
_mapper_instance = None


def get_caste_mapper():
    """
    Get singleton instance of CasteMapper.
    
    Returns:
        CasteMapper: Singleton instance
    """
    global _mapper_instance
    if _mapper_instance is None:
        _mapper_instance = CasteMapper()
    return _mapper_instance


def map_surname_to_caste(surname):
    """
    Convenience function to map a surname to caste group.
    
    Args:
        surname (str): Surname
    
    Returns:
        str: Caste group
    """
    mapper = get_caste_mapper()
    return mapper.get_caste_group(surname)


# Example usage
if __name__ == '__main__':
    # This would work in Django shell
    pass