from typing import Dict, List, Optional, Tuple, Set
import xml.etree.ElementTree as ET
import omero.scripts as scripts
from omero.gateway import BlitzGateway, FileAnnotationWrapper
from omero.rtypes import rstring, rint, robject, rlong
import omero
from omero.util.populate_roi import DownloadingOriginalFileProvider
from omero.model import LengthI
from omero.model.enums import UnitsLength

# Configuration defaults
DEFAULT_CONFIG = {
    'delete_source': False,
    'plate_name_template': None,
    'batch_size': 100
}

class PlateCreator:
    def __init__(self, conn):
        self.conn = conn
        self.update_service = conn.getUpdateService()
        
    def create_well(self, plate, row, col, fields, dataset) -> omero.model.WellI:
        """Create a well with all its fields/samples"""
        well = omero.model.WellI()
        well.plate = plate
        well.row = rint(row)
        well.column = rint(col)
        
        for field in fields:
            self._add_field_to_well(well, field, dataset)
            
        return self.update_service.saveAndReturnObject(well)
        
    def _add_field_to_well(self, well, field, dataset) -> None:
        """Add a single field/sample to a well"""
        image = get_image_by_name(dataset, field['filename'])
        if image:
            ws = omero.model.WellSampleI()
            ws.image = omero.model.ImageI(image.getId(), False)
            ws.well = well
            well.addWellSample(ws)

def get_image_by_name(dataset, image_name: str) -> Optional[omero.gateway.ImageWrapper]:
    """Find image in dataset by name with caching"""
    if not hasattr(dataset, '_image_cache'):
        dataset._image_cache = {
            img.getName(): img for img in dataset.listChildren()
        }
    return dataset._image_cache.get(image_name)

def parse_xml_metadata(xml_path: str) -> Dict[str, any]:
    """Parse XML file containing plate layout information.
    
    Args:
        xml_path: Path to XML file containing plate layout
        
    Returns:
        Dictionary containing:
            - plate_name: str
            - row_naming: str
            - col_naming: str
            - well_mappings: Dict[Tuple[int, int], List[Dict]]
            - image_names: Set[str]
            
    Raises:
        Exception: If XML is invalid or missing required elements
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()
    namespace = {'ome': root.tag.split('}')[0].strip('{')}
    
    plate_elem = root.find('.//ome:Plate', namespace)
    if plate_elem is None:
        raise Exception("No Plate element found in XML")
        
    plate_name = plate_elem.get('Name')
    row_naming = plate_elem.get('RowNamingConvention')
    col_naming = plate_elem.get('ColumnNamingConvention')
    
    image_map = {}
    for image in root.findall('.//ome:Image', namespace):
        image_id = image.get('ID')
        uuid_elem = image.find('.//ome:UUID', namespace)
        if uuid_elem is not None:
            filename = uuid_elem.get('FileName')
            image_map[image_id] = filename
    
    well_mappings = {}
    image_names = set()
    
    for well in plate_elem.findall('.//ome:Well', namespace):
        row = int(well.get('Row'))
        col = int(well.get('Column'))
        well_key = (row, col)
        
        fields = []
        for wellsample in well.findall('.//ome:WellSample', namespace):
            image_ref = wellsample.find('.//ome:ImageRef', namespace)
            if image_ref is not None:
                image_id = image_ref.get('ID')
                if image_id in image_map:
                    fields.append({
                        'index': int(wellsample.get('Index')),
                        'filename': image_map[image_id]
                    })
                    image_names.add(image_map[image_id])
        
        if fields:
            well_mappings[well_key] = fields
    
    print(f"\nParsed XML metadata:")
    print(f"Plate name: {plate_name}")
    print(f"Row naming: {row_naming}")
    print(f"Column naming: {col_naming}")
    print(f"Number of wells: {len(well_mappings)}")
    print("Well mappings:")
    for (row, col), fields in well_mappings.items():
        print(f"Well ({row},{col}): {[f['filename'] for f in fields]}")
    
    return {
        'plate_name': plate_name,
        'row_naming': row_naming,
        'col_naming': col_naming,
        'well_mappings': well_mappings,
        'image_names': image_names
    }

def create_plate_from_dataset(conn, dataset_id: int, file_ann_id: int, plate_name_template: Optional[str] = None) -> omero.model.PlateI:
    """Create a new plate from dataset images using XML layout"""
    try:
        dataset = conn.getObject("Dataset", dataset_id)
        if not dataset:
            raise Exception(f"Dataset {dataset_id} not found")
            
        print(f"\nProcessing Dataset: {dataset.getName()} (ID:{dataset.getId()})")
        
        # Get and parse XML file
        original_file = get_original_file(dataset, file_ann_id)
        provider = DownloadingOriginalFileProvider(conn)
        xml_path = provider.get_original_file_data(original_file)
        metadata = parse_xml_metadata(xml_path.name)
        xml_path.close()
        
        # Validate well mappings
        if not metadata['well_mappings']:
            raise Exception("No valid well mappings found in XML")
            
        # Validate image existence
        missing_images = []
        for fields in metadata['well_mappings'].values():
            for field in fields:
                if not get_image_by_name(dataset, field['filename']):
                    missing_images.append(field['filename'])
        
        if missing_images:
            raise Exception(f"Missing images: {', '.join(missing_images)}")
        
        print("\nSuccessfully parsed XML metadata")
        print(f"Plate name: {metadata['plate_name']}")
        print(f"Number of wells: {len(metadata['well_mappings'])}")
        
        # Create plate
        plate_name = metadata['plate_name']
        if plate_name_template:
            plate_name = plate_name_template.format(
                dataset_name=dataset.getName(),
                plate_name=metadata['plate_name']
            )
            
        plate = omero.model.PlateI()
        plate.name = rstring(plate_name)
        
        if metadata['row_naming']:
            plate.rowNamingConvention = rstring(metadata['row_naming'])
        if metadata['col_naming']:
            plate.columnNamingConvention = rstring(metadata['col_naming'])
            
        plate_creator = PlateCreator(conn)
        plate = conn.getUpdateService().saveAndReturnObject(plate)
        
        # Create wells and add images
        for (row, col), fields in metadata['well_mappings'].items():
            plate_creator.create_well(plate, row, col, fields, dataset)
            
        return plate
        
    except Exception as e:
        raise Exception(f"Error in create_plate_from_dataset: {str(e)}")

def get_original_file(omero_object, file_ann_id=None):
    """Find file linked to object. Option to filter by ID."""
    if not hasattr(omero_object, 'OMERO_CLASS'):
        raise Exception(f"Invalid OMERO object provided: {type(omero_object)}")
        
    file_ann = None
    print(f"\nSearching for XML file annotation on {omero_object.OMERO_CLASS} ID:{omero_object.getId()}")
    
    # Ensure file_ann_id is an integer
    if file_ann_id is not None:
        file_ann_id = int(file_ann_id)
        print(f"Looking for File Annotation ID: {file_ann_id} (type: {type(file_ann_id)})")
    
    annotations = list(omero_object.listAnnotations())
    print(f"Found {len(annotations)} total annotations")
    
    for ann in annotations:
        if isinstance(ann, omero.gateway.FileAnnotationWrapper):
            current_id = ann.getId()
            current_name = ann.getFile().getName()
            print(f"Checking file annotation: ID={current_id} (type: {type(current_id)}), Name={current_name}")
            
            if file_ann_id is not None and current_id == file_ann_id:
                print(f"✓ Found matching file annotation ID: {current_id}")
                file_ann = ann
                break
    
    if file_ann is None:
        raise Exception("No matching file annotation found")
        
    print(f"\nSuccessfully found file annotation:")
    print(f"ID: {file_ann.getId()}")
    print(f"Name: {file_ann.getFile().getName()}")
    
    return file_ann.getFile()._obj

def run_script():
    """The main script execution function."""
    data_types = [rstring('Dataset')]
    client = scripts.client(
        'Create_Plate_From_Dataset',
        """This script creates a new plate using images from a dataset, 
        organized according to an XML file.""",
        scripts.String(
            "Data_Type", optional=False, grouping="1",
            description="The script only works with Datasets",
            values=data_types, default="Dataset"),
        scripts.List(
            "IDs", optional=False, grouping="2",
            description="List of Dataset IDs to process.").ofType(rlong(0)),
        scripts.String(
            "Header_File", grouping="3.1",
            description="File ID containing header_file."),
        scripts.String(
            "Header_File_Path", optional=True, grouping="3.2",
            description="The path to your header file on the server. Optional.",
            namespaces=[omero.constants.namespaces.NSDYNAMIC]),
        scripts.String(
            "Plate_Name", grouping="4", optional=True,
            description="Template for plate names. Available variables: "
            "{dataset_name}, {plate_name}"),
        scripts.Bool(
            "Delete_Source", optional=True, grouping="5",
            description="Delete source datasets after plate creation",
            default=False),
        authors=["Your Name"],
        institutions=["Your Institution"],
        contact="your@email.com"
    )

    try:
        # Get script parameters and create config
        script_params = {}
        for key in client.getInputKeys():
            if client.getInput(key):
                script_params[key] = client.getInput(key, unwrap=True)
                
        config = DEFAULT_CONFIG.copy()
        config.update({
            'delete_source': script_params.get("Delete_Source", False),
            'plate_name_template': script_params.get("Plate_Name")
        })

        conn = BlitzGateway(client_obj=client)
        dataset_ids = script_params["IDs"]
        header_file_id = script_params.get("Header_File")
        header_file_path = script_params.get("Header_File_Path")
        
        print("\nScript parameters:")
        print(f"Dataset IDs: {dataset_ids}")
        print(f"Header File ID: {header_file_id}")
        print(f"Header File Path: {header_file_path}")
        
        errors = []
        plates_created = []
        
        for dataset_id in dataset_ids:
            try:
                # Get dataset object first
                dataset = conn.getObject("Dataset", dataset_id)
                if not dataset:
                    raise Exception(f"Dataset {dataset_id} not found")
                
                if header_file_id:
                    link_file_ann(conn, "Dataset", dataset_id, header_file_id)
                
                plate = create_plate_from_dataset(
                    conn, dataset_id, header_file_id, config['plate_name_template'])
                plates_created.append(plate.getId().getValue())
                
                if config['delete_source']:
                    conn.deleteObjects('Dataset', [dataset_id], 
                                     deleteAnns=True, deleteChildren=True)
            except Exception as e:
                errors.append(f"Dataset {dataset_id}: {str(e)}")
        
        message = []
        if plates_created:
            message.append(f"Created {len(plates_created)} plates")
        if errors:
            message.append("\nErrors:")
            message.extend(errors)
            
        client.setOutput("Message", rstring("\n".join(message)))
        if plates_created:
            client.setOutput("Plates_Created", omero.rtypes.wrap(plates_created))

    finally:
        client.closeSession()

def link_file_ann(conn, object_type, object_id, file_ann_id):
    """Link File Annotation to the Object, if not already linked."""
    print(f"\nLinking file annotation {file_ann_id} to {object_type} {object_id}")
    
    file_ann = conn.getObject("Annotation", file_ann_id)
    if file_ann is None:
        raise Exception(f"File Annotation not found: {file_ann_id}")
    
    omero_object = conn.getObject(object_type, object_id)
    if omero_object is None:
        raise Exception(f"{object_type} {object_id} not found")
    
    # Check for existing links
    links = list(conn.getAnnotationLinks(object_type, parent_ids=[object_id],
                                       ann_ids=[file_ann_id]))
    if len(links) == 0:
        print(f"Creating new link between {object_type} {object_id} and File Annotation {file_ann_id}")
        omero_object.linkAnnotation(file_ann)
    else:
        print(f"Link already exists between {object_type} {object_id} and File Annotation {file_ann_id}")

if __name__ == "__main__":
    run_script()