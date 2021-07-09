"""
Afterglow Core: FTP data provider plugin
"""

from ftplib import FTP, error_perm
from typing import List as TList
from datetime import datetime

import gzip
import astropy.io.fits as astro
import sunpy.io.fits as sun

from io import BytesIO

from afterglow_core.models import DataProvider, DataProviderAsset
from afterglow_core.errors.data_provider import AssetNotFoundError


__all__ = ['FTPDataProvider']


class FTPDataProvider(DataProvider):
    """
   File transfer protocol data provider plugin class
    """
    name = 'FTP_Service'
    display_name = 'GONG File Transfer'
    description = 'Access to the GONG database of solar observations'

    readonly = True
    quota = usage = False
    allow_multiple_instances = True
    
    #ftp sign in information
    ftp_host = 'gong2.nso.edu'
    user = 'anonymous'
    password = 'afterglow@unc.edu'
    
    def get_asset(self, path: str) -> DataProviderAsset:
        """
        Return an asset at the given path
        :param path: asset path
        :return: asset object
        """
        
        name = '' #Find the  basename from the path
        for i in path:
            if i == '/':
                name = ''
            else:
                name += i
        
        ftp = FTP(self.ftp_host)
        ftp.login(self.user,self.password)
        
        #determine if the asset is collection
        collection = bool
        try:
            ftp.cwd(path) #raises exception if path is not a directory or does not exist
            collection = True
        except error_perm:
            if ftp.nlst(path)[0]:
                collection = False
            else:
                raise AssetNotFoundError()
    
        metadata = dict(time=self._get_date(path))
        
        #Header data of a NON-collection asset. GONG files are always fits.
        if not collection:
            ftype ='FITS'
            size = ftp.size(path)
            
            hdu = astro.HDUList.fromstring(self.get_asset_data(path))
            head = sun.get_header(hdu)
            
            imwidth = head[0]['NAXIS1']
            imheight = head[0]['NAXIS2']
            
            try:
                telescope = head[0]['SITENAME']
            except KeyError:
                pass
            
            try:
                try:
                    exptime = datetime.strptime(
                    head[0]['DATE-OBS'], '%Y-%m-%dT%H:%M:%S.%f')
                except ValueError:
                    try:
                        exptime = datetime.strptime(
                        head[0]['DATE-OBS'], '%Y-%m-%dT%H:%M:%S')
                    except ValueError:
                        try:
                            exptime = datetime.strptime(
                            head[0]['DATE-OBS'] + 'T' +
                            head[0]['TIME-OBS'],
                            '%Y-%m-%dT%H:%M:%S.%f')
                        except ValueError:
                            try:
                                exptime = datetime.strptime(
                                head[0]['DATE-OBS'] + 'T' +
                                head[0]['TIME-OBS'],
                                '%Y-%m-%dT%H:%M:%S')
                            except ValueError:
                                exptime = self._get_date(path)
            except ValueError:
                pass
            
            if ftype is not None:
                metadata['type'] = ftype
            if size is not None:
                metadata['size'] = size
            if exptime is not None:
                metadata['time'] = exptime
            if imwidth is not None:
                metadata['width'] = imwidth
                metadata['height'] =imheight
            if telescope is not None:
                metadata['telescope'] = telescope
                
        ftp.quit()
        return DataProviderAsset(
            name=name,
            collection=collection,
            path=path,
            metadata=metadata)
    
    def _get_date(self, path: str) -> datetime:
        """
        Used internally by get_asset and get_child_assets. If only I could _get_date.
        :param path: asset path
        :return: date object
        """

        
        #Observation Time
        #this obtains the observation date/time for GONG. This ONLY works with the way GONG names their files.
        name = '' #Find the  basename from the path
        for i in path:
            if i == '/':
                name = ''
            else:
                name += i
        
        i=0
        datestr = ''
        try:
            while not name[i] in '0123456789' and i<len(name): #get rid of any letters at the start
                i+=1
        
            if name[i]:
                while i<len(name):
                    datestr += name[i]
                    i+=1
                
                if len(name)==6: #certain directories are named by the year and month of the observations inside (YYYYMM)
                    date = datetime( int(datestr[0:4]) , int(datestr[4:6]) ,1)
                    
                elif len(name)==12: #others are named by the Location of observation and year, month, and day (LLYYMMDD)
                    date = datetime( 2000+int(datestr[0:2]) , int(datestr[2:4]) , int(datestr[4:6]) )
                    
                else: #actual files have an additional time on the end
                     date = datetime( 2000+int(datestr[0:2]) , int(datestr[2:4]) , int(datestr[4:6]) , int(datestr[7:9] , int(datestr[9:11])))
        
        except:
            date = None
        
        return date
        
    def get_asset_data(self, path) -> bytes:
        """
        Return data for a non-collection asset at the given path
        :param path: asset path; must identify a non-collection asset
        :return: asset data
        """
        ftp = FTP(self.ftp_host)
        ftp.login(self.user,self.password)
        
        directory = ''
        for i in path:
            if i == '/':
                ftp.cwd(directory)
                directory = ''
            else:
                directory += i
        
        b=[]
        try:
            ftp.retrbinary('RETR ' + directory,b.append)
        except Exception:
            raise AssetNotFoundError
        
        ftp.quit()
        
        buf = BytesIO()
        for i in b:
            buf.write(i)
        
        return gzip.decompress(buf.getvalue())
            
        '''
        #Write helioprojective coordinate system to header
        hdul = astro.HDUList.fromstring(gzip.decompress(buf.getvalue()))
        hdr = hdul[0].header
        
        hdr['']
        '''
        
    def get_child_assets(self, path) -> TList[DataProviderAsset]:
        
        ftp = FTP(self.ftp_host)
        ftp.login(self.user,self.password)
        
        try:
            ftp.cwd(path) #raises exception if path is not a directory or does not exist
        except Exception:
            raise AssetNotFoundError()
        
        flist = ftp.nlst()
        assets = []
        for f in flist:
            
            fpath = '/'+path+'/'+f
            
            #determine if the asset is collection
            collection = False
            try:
                ftp.cwd(fpath) #raises exception if path is not a directory or does not exist
                collection = True
                ftp.cwd(path)
            except error_perm:
                pass
        
            metadata = dict(time=self._get_date(fpath))
            if not collection:
                metadata['size'] = ftp.size(f)
            
            assets.append(DataProviderAsset(
                name=f,
                collection=collection,
                path=fpath,
                metadata=metadata))
        ftp.quit()
        return assets
