from models import *
from settings import MANAGED_HOST_TAGS_INPUT_FILE, MANAGED_IS_NAMES_INPUT_FILE


managed_host_tags=[]
with open(MANAGED_HOST_TAGS_INPUT_FILE, 'r') as file:
        for line in file:
            managed_host_tags.extend(line.strip().split(','))
managed_is_names=[]
with open(MANAGED_IS_NAMES_INPUT_FILE, 'r') as file:
        for line in file:
            managed_is_names.extend(line.strip().split(','))  

def host_is_managed_by_tags(tags: str, managed_tags: list=managed_host_tags) -> bool:
    """
    Determine if an entity is managed based on its tags.
    Checks if any tag from managed_host_tags.txt matches the entity's tags.
    
    Args:
        tags: String containing all tags for the entity
        managed_tags: List of managed tags from managed_host_tags.txt
        
    Returns:
        bool: True if entity has any managed tag, False otherwise
    """
    # Convert tags string to lowercase for case-insensitive comparison
    tags_lower = str(tags).lower()
    
    # Check if any managed tag exists in the entity's tags
    for managed_tag in managed_tags:
        if managed_tag.lower().strip() in tags_lower:
            return True
    return False

def IS_is_managed_by_name(name: str, managed_names: list=managed_is_names) -> bool:
    """
    Determine if an entity is managed based on its name.
    Checks if name exists in managed_is_names.txt.
    
    Args:
        name: Name of the entity
        managed_names: List of managed names from managed_is_names.txt
        
    Returns:
        bool: True if name is in managed names list, False otherwise
    """
    # Convert to lowercase for case-insensitive comparison
    name_lower = name.lower().strip()
    managed_names_lower = [n.lower().strip() for n in managed_names]
    
    return name_lower in managed_names_lower




# ===== CUSTOM LOGIC FOR MANAGED ENTITIES =====
# These functions determine if an entity is managed by DIGIT

def is_is_managed(is_name: str, managed_names: list) -> bool:
    """
    Determine if an IS is managed by DIGIT based on its name.
    """
    return IS_is_managed_by_name(is_name, managed_names)

def host_is_managed(host:Host, managed_tags: list) -> bool:
    """
    Determine if a host is managed by DIGIT based on its tags.
    """
    return host_is_managed_by_tags(host.tags, managed_tags)

def synthetic_is_managed(synthetic:Synthetic)->bool:
    """
    Determine if a synthetic monitor is managed by DIGIT.
    Currently no synthetic monitors are considered managed.
    """
    return False

def application_is_managed(application:Application)->bool:
    """
    Determine if an application is managed by DIGIT.
    Currently no applications are considered managed.
    """
    return False

# ===== CUSTOM LOGIC FOR BILLABLE ENTITIES =====
# These functions determine if an entity should be billed
# An entity is billable if it or any of its information systems are managed
# However, if the entity itself is managed, it is not billable

def host_is_billable(host:Host)->bool:
    """
    Determine if a host should be billed.
    A host is not billable if it is managed or if any of the information systems it belongs to are managed.
    """
    billable = False if host.managed or any(is_.managed for is_ in host.information_systems) else True
    return billable

def application_is_billable(application:Application)->bool:
    """
    Determine if an application should be billed.
    An application is not billable if it is managed or if any of the information systems it belongs to are managed.
    """
    billable = False if application.managed or any(is_.managed for is_ in application.information_systems) else True
    return billable

def synthetic_is_billable(synthetic:Synthetic)->bool:
    """
    Determine if a synthetic monitor should be billed.
    A synthetic monitor is not billable if it is managed or if any of the information systems it belongs to are managed.
    """
    billable = False if synthetic.managed or any(is_.managed for is_ in synthetic.information_systems) else True
    return billable