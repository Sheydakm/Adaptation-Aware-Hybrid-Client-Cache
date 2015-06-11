"""
    Module for reading the MPD file generated by GPAC version 0.5.1-DEV-rev5379
    Author: Parikshit Juluri
    Contact : pjuluri@umkc.edu


"""
from __future__ import division
import re
import config_client

# Dictionary to convert size to bits
SIZE_DICT = {'bits':   1,
             'Kbits':  1024,
             'Mbits':  1024*1024,
             'bytes':  8,
             'KB':  1024*8,
             'MB': 1024*1024*8,
             }
# Try to import the C implementation of ElementTree which is faster
# In case of ImportError import the pure Python implementation
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

MEDIA_PRESENTATION_DURATION = 'mediaPresentationDuration'
MIN_BUFFER_TIME = 'minBufferTime'

def get_tag_name(xml_element):
    """ Module to remove the xmlns tag from the name
        eg: '{urn:mpeg:dash:schema:mpd:2011}SegmentTemplate'
             Return: SegmentTemplate
    """
    try:
        tag_name = xml_element[xml_element.find('}')+1:]
    except TypeError:
        config_client.LOG.error("Unable to retrieve the tag. ")
        return None
    return tag_name


def get_playback_time(playback_duration):
    """ Get the playback time(in seconds) from the string:
        Eg: PT0H1M59.89S
    """
    # Get all the numbers in the string
    numbers = re.split('[PTHMS]', playback_duration)
    # remove all the empty strings
    numbers = [value for value in numbers if value != '']
    numbers.reverse()
    total_duration = 0
    for count, val in enumerate(numbers):
        if count == 0:
            total_duration += float(val)
        elif count == 1:
            total_duration += float(val) * 60
        elif count == 2:
            total_duration += float(val) * 60 * 60
    return total_duration


#class MediaObject(object):
#    """Object to handel audio and video stream """
#    def __init__(self):
#        self.min_buffer_time = None
#        self.start = None
#        self.timescale = None
#        self.segment_duration = None
#        self.initialization = None
#        self.base_url = None
#        self.url_list = list()

class DashPlayback:
    """ 
    Media Playback object that holds the metadata retrieved from the MPD file
    Audio[bandwidth] : {duration, url_list}
    Video[bandwidth] : {duration, url_list}
    """
    def __init__(self):
        self.min_buffer_time = None
        self.playback_duration = None
        self.audio = dict()
        self.video = {'base_url': None,
                      'start': None,
                      'timescale': None,
                      'initialization': None,
                      'duration': None,
                      'bandwidth_list': []
                      }

def get_segment_path(media, playback_duration, bitrate, segment_number):
    """
    :param media: Instance of MediaObject
    :param playback_duration: Duration in seconds
    :param bitrate: The bandwidth for the segment
    :param segment_number: The segment number
    :return: The path to the segment for the given bitrate
    """
    base_url = media['base_url']
    segment_path = None
    if "$Bandwidth$" in base_url:
        base_url = base_url.replace("$Bandwidth$", str(bitrate))
    if "$Number" in base_url:
        segment_duration = media['duration']/media['timescale']
        if (segment_number * segment_duration) - segment_duration < playback_duration:
            segment_path = base_url.replace('$Number$', str(segment_number))
    return segment_path


def read_mpd(mpd_file):
    """
     Module to parse the MPD file after it is downloaded
    :param mpd_file: Path to the MPD file on the local machine
    :param dashplayback: Instance of the DashPlayback Object
    :return: DashPayback object with the parsed data
    """
    dash_playback_object = DashPlayback()
    config_client.LOG.info("Reading the MPD file")
    try:
        tree = ET.parse(mpd_file)
    except IOError:
        config_client.LOG.error("MPD file not found. Exiting")
        return None
    config_client.JSON_HANDLE["video_metadata"] = {'mpd_file': mpd_file}
    root = tree.getroot()
    if 'MPD' in get_tag_name(root.tag).upper():
        if MEDIA_PRESENTATION_DURATION in root.attrib:
            dash_playback_object.playback_duration = get_playback_time(root.attrib[MEDIA_PRESENTATION_DURATION])
            config_client.JSON_HANDLE["video_metadata"]['playback_duration'] = dash_playback_object.playback_duration
        if MIN_BUFFER_TIME in root.attrib:
            dash_playback_object.min_buffer_time = get_playback_time(root.attrib[MIN_BUFFER_TIME])
    for element in root:
        if 'Period' in get_tag_name(element.tag):
            for adaptation_set in element:
                for representation in adaptation_set:
                    if "SegmentTemplate" in get_tag_name(representation.tag):
                                dash_playback_object.video['base_url'] = representation.attrib['media']
                                config_client.LOG.info('base_url = {}'.format(representation.attrib['media']))
                                dash_playback_object.video['start'] = int(representation.attrib['startNumber'])
                                config_client.LOG.info('start = {}'.format(representation.attrib['startNumber']))
                                dash_playback_object.video['timescale'] = float(representation.attrib['timescale'])
                                config_client.LOG.info('timescale = {}'.format(representation.attrib['timescale']))
                                dash_playback_object.video['initialization'] = representation.attrib['initialization']
                                config_client.LOG.info('initialization = {}'.format(representation.attrib['initialization']))
                                dash_playback_object.video['duration'] = int(representation.attrib['duration'])
                                config_client.LOG.info('duration  = {}'.format(representation.attrib['duration']))
                    elif 'video' in representation.attrib['mimeType']:
                        bandwidth = int(representation.attrib['bandwidth'])
                        dash_playback_object.video['bandwidth_list'].append(bandwidth)
    if len(dash_playback_object.video['bandwidth_list']) > 0:
        config_client.JSON_HANDLE['playback_info']['available_bitrates'] = dash_playback_object.video['bandwidth_list']
        bitrates_str = [str(i) for i in dash_playback_object.video['bandwidth_list']]
        config_client.LOG.info('The video has the following bitrates: {}'.format(','.join(bitrates_str)))
    return dash_playback_object