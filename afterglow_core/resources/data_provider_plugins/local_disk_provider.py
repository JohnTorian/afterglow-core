"""
Afterglow Core: local disk data provider plugin
"""

import sys
import os
import shutil
import gzip
import bz2
from errno import EEXIST
from datetime import datetime
from glob import glob
from typing import List as TList, Optional, Union

import astropy.io.fits as pyfits

try:
    from PIL import Image as PILImage, ExifTags
except ImportError:
    PILImage = ExifTags = None

try:
    import rawpy
except ImportError:
    rawpy = None

try:
    import exifread
except ImportError:
    exifread = None

from ... import auth, errors
from ...models import DataProvider, DataProviderAsset
from ...errors.data_provider import (
    AssetNotFoundError, AssetAlreadyExistsError,
    CannotUpdateCollectionAssetError)
from ...errors.data_provider_local_disk import *
from ...errors.data_file import UnrecognizedDataFormatError


__all__ = ['LocalDiskDataProvider', 'RestrictedRWLocalDiskDataProvider']


class LocalDiskDataProvider(DataProvider):
    """
    Local disk data provider plugin class
    """
    name = 'local_disk'
    display_name = description = 'Local Filesystem'

    search_fields = dict(
        type=dict(label='Data File Type', type='multi_choice', enum=['FITS']),
        name=dict(label='File Name Pattern', type='text'),
        width=dict(label='Image Width', type='int', min_val=1),
        height=dict(label='Image Height', type='int', min_val=1),
    )
    if PILImage is not None:
        search_fields['type']['enum'] += ['JPEG', 'PNG', 'TIFF']
    if rawpy is not None:
        search_fields['type']['enum'].append('RAW')

    peruser: bool = False
    root: str = '.'

    @property
    def usage(self) -> int:
        """
        Return disk space usage

        :return: number of bytes within the data root directory
        """
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.abs_root):
            for f in filenames:
                total_size += os.path.getsize(os.path.join(dirpath, f))
        return total_size

    @property
    def abs_root(self) -> str:
        """
        Return the absolute path to the local disk data provider root directory

        :return: local root directory path
        """
        p = os.path.abspath(os.path.expanduser(self.root))
        if self.peruser:
            user_id = auth.current_user.id
            if user_id:
                p = os.path.join(p, str(user_id))

                if not os.path.exists(p):
                    # In read-write per-user ("workspace") mode, make sure that
                    # the user workspace directory exists
                    try:
                        try:
                            os.makedirs(p)
                        except OSError as e:
                            if e.errno != EEXIST:
                                raise
                    except Exception as e:
                        # noinspection PyUnresolvedReferences
                        raise FilesystemError(reason=str(e))
        return p

    def _path_to_filename(self, path: str) -> str:
        """
        Return absolute filesystem name corresponding to the given path
        and check that it is inside the data provider root

        :param path: asset path

        :return: fully-qualified filesystem path
        """
        root = self.abs_root
        filename = os.path.abspath(os.path.join(root, path.strip('/')))
        if not filename.startswith(root):
            raise AssetOutsideRootError()
        return filename

    @staticmethod
    def _get_asset(path: str, filename: str) -> DataProviderAsset:
        """
        Return an asset at the given path with no extra checks; used by
        other class methods and is not meant for external use

        :param path: asset path
        :param filename: full filesystem path to asset

        :return: asset object
        """
        path = path.replace('\\', '/').strip('/')
        name = os.path.basename(filename)

        if os.path.isdir(filename):
            # Collection asset
            return DataProviderAsset(
                name=name,
                collection=True,
                path=path,
                metadata=dict(
                    time=datetime.fromtimestamp(
                        os.stat(filename).st_mtime).isoformat(),
                )
            )

        # Asset is a file; try to read it and get metadata
        imtype = layers = imwidth = imheight = None
        explength = exptime = telescope = flt = None

        # A FITS file?
        # noinspection PyBroadException
        try:
            with pyfits.open(filename, 'readonly') as f:
                layers = len(f)
                imwidth = f[0].header['NAXIS1']
                imheight = f[0].header['NAXIS2']

                try:
                    explength = f[0].header['EXPOSURE']
                except KeyError:
                    pass

                try:
                    telescope = f[0].header['TELESCOP']
                except KeyError:
                    pass

                flt = ','.join(hdu.header['FILTER']
                               for hdu in f if 'FILTER' in hdu.header)

                try:
                    try:
                        exptime = datetime.strptime(
                            f[0].header['DATE-OBS'], '%Y-%m-%dT%H:%M:%S.%f')
                    except ValueError:
                        try:
                            exptime = datetime.strptime(
                                f[0].header['DATE-OBS'], '%Y-%m-%dT%H:%M:%S')
                        except ValueError:
                            try:
                                exptime = datetime.strptime(
                                    f[0].header['DATE-OBS'] + 'T' +
                                    f[0].header['TIME-OBS'],
                                    '%Y-%m-%dT%H:%M:%S.%f')
                            except ValueError:
                                try:
                                    exptime = datetime.strptime(
                                        f[0].header['DATE-OBS'] + 'T' +
                                        f[0].header['TIME-OBS'],
                                        '%Y-%m-%dT%H:%M:%S')
                                except ValueError:
                                    pass
                except KeyError:
                    pass

            imtype = 'FITS'
        except Exception:
            pass

        if imtype is None and (PILImage is not None or rawpy is not None):
            with open(filename, 'rb') as f:
                exif = None

                if PILImage is not None:
                    from ..data_files import convert_exif_field
                    # noinspection PyBroadException
                    try:
                        with PILImage.open(f) as im:
                            imtype = im.format
                            band_names = im.getbands()
                            layers = len(band_names)
                            flt = ','.join(band_names)
                            imwidth, imheight = im.size
                            exif = {
                                ExifTags.TAGS[key]: convert_exif_field(val)
                                for key, val in im.getexif().items()
                            }
                    except Exception:
                        pass

                if imtype is None and rawpy is not None:
                    # noinspection PyBroadException
                    try:
                        # Intercept stderr to disable rawpy warnings on non-raw
                        # files
                        save_stderr = sys.stderr
                        sys.stderr = os.devnull
                        try:
                            f.seek(0)
                            # noinspection PyTypeChecker
                            im = rawpy.imread(f)
                        finally:
                            sys.stderr = save_stderr
                        try:
                            imtype = str(im.raw_type)
                            layers = im.num_colors
                            flt = ','.join(chr(b) for b in im.color_desc)
                            imwidth = im.sizes.width
                            imheight = im.sizes.height
                        finally:
                            im.close()
                    except Exception:
                        pass

                if imtype is not None and exifread is not None:
                    from ..data_files import convert_exif_field
                    # noinspection PyBroadException
                    try:
                        # Use ExifRead when available; remove "EXIF "
                        # etc. prefixes
                        f.seek(0)
                        exif = {
                            key.split(None, 1)[-1]: convert_exif_field(val)
                            for key, val in exifread.process_file(f).items()}
                    except Exception:
                        pass

                if exif is not None:
                    # Exposure length
                    # noinspection PyBroadException
                    try:
                        explength = exif['ExposureTime']
                    except Exception:
                        pass

                    # Exposure time
                    try:
                        exptime = exif['DateTime']
                    except KeyError:
                        try:
                            exptime = exif['DateTimeOriginal']
                        except KeyError:
                            try:
                                exptime = exif['DateTimeDigitized']
                            except KeyError:
                                pass
                    if exptime:
                        try:
                            exptime = datetime.strptime(
                                str(exptime), '%Y:%m:%d %H:%M:%S.%f')
                        except ValueError:
                            try:
                                exptime = datetime.strptime(
                                    str(exptime), '%Y:%m:%d %H:%M:%S')
                            except ValueError:
                                pass

        if imtype is None:
            # Unrecognized file
            raise UnrecognizedDataFormatError()

        stat = os.stat(filename)
        asset = DataProviderAsset(
            name=name,
            collection=False,
            path=path,
            metadata=dict(
                type=imtype,
                size=stat.st_size,
                time=(exptime if exptime is not None
                      else datetime.fromtimestamp(stat.st_mtime)).isoformat(),
                layers=layers,
                width=imwidth, height=imheight,
            ),
        )
        if explength is not None:
            asset.metadata['exposure'] = explength
        if telescope is not None:
            asset.metadata['telescope'] = telescope
        if flt is not None:
            asset.metadata['filter'] = flt

        return asset

    def get_asset(self, path: str) -> DataProviderAsset:
        """
        Return an asset at the given path

        :param path: asset path

        :return: asset object
        """
        filename = self._path_to_filename(path)
        if not os.path.exists(filename):
            raise AssetNotFoundError(path=path)

        return self._get_asset(path, filename)

    def get_child_assets(self, path: str) -> TList[DataProviderAsset]:
        """
        Return child assets of a collection asset at the given path

        :param path: asset path; must identify a collection asset

        :return: list of :class:`DataProviderAsset` objects for child assets
        """
        filename = self._path_to_filename(path)
        if not os.path.isdir(filename):
            raise AssetNotFoundError(path=path)

        # Return directory contents
        root = self.abs_root
        return [DataProviderAsset(
            name=os.path.basename(fn),
            collection=os.path.isdir(fn),
            path=fn.split(root + os.path.sep)[1].replace('\\', '/'),
            metadata=dict(
                time=datetime.fromtimestamp(os.stat(fn).st_mtime).isoformat(),
            )
        ) for fn in glob(os.path.join(filename, '*'))]

    def find_assets(self, path: Optional[str] = None,
                    name: Optional[str] = None,
                    type: Optional[str] = None,
                    collection: Optional[Union[str, int, bool]] = None,
                    width: Optional[Union[str, int]] = None,
                    height: Optional[Union[str, int]] = None) \
            -> TList[DataProviderAsset]:
        """
        Return a list of assets matching the given parameters

        :param path: optional path to the collection asset to search in;
            by default, search in the data provider root
        :param name: only return assets matching the given name; may include
            wildcards
        :param type: comma-separated list of data types ("FITS", "JPEG", etc.);
            if specified, the query will match only data files of the given
            type(s)
        :param collection: if specified, match only the given asset type
            (True | "1" = directories, False | "0" = files)
        :param width: match only images of the given width
        :param height: match only images of the given height

        :return: list of :class:`DataProviderAsset` objects for assets matching
            the search query parameters
        """
        # Set up filters
        if type:
            type = type.split(',')
        else:
            type = None
        if collection:
            try:
                collection = bool(int(collection))
            except ValueError:
                raise errors.ValidationError(
                    'collection', 'Collection flag must be 0 or 1')
        else:
            collection = None
        if width:
            try:
                width = int(width)
            except ValueError:
                raise errors.ValidationError('width', 'Width must be integer')
        else:
            width = None
        if height:
            try:
                height = int(height)
            except ValueError:
                raise errors.ValidationError('height', 'Height must be integer')
        else:
            height = None

        root = self.abs_root
        if path is None:
            # Search at the data root by default
            path = ''
            abs_path = root
        else:
            abs_path = self._path_to_filename(path)

            # Prevent from going above the root path
            if not abs_path.startswith(root):
                raise AssetOutsideRootError()

        if not os.path.isdir(abs_path):
            raise AssetNotFoundError(path=path)

        # Look through all files within the path matching the given name
        assets = []
        if not name:
            name = '*'
        for filename in glob(os.path.join(abs_path, name)):
            if collection is not None and os.path.isdir(filename) != collection:
                # Fast path for searching collection or non-collection assets
                continue

            try:
                asset = self._get_asset(
                    filename.split(root + os.path.sep)[1], filename)
            except errors.AfterglowError:
                # Not a supported data file
                continue

            # Check other search filters
            if type is not None:
                try:
                    if asset.metadata['type'] not in type:
                        continue
                except KeyError:
                    continue

            if width is not None:
                try:
                    if asset.metadata['width'] != width:
                        continue
                except KeyError:
                    continue

            if height is not None:
                try:
                    if asset.metadata['height'] != height:
                        continue
                except KeyError:
                    continue

            # All checks passed
            assets.append(asset)

        return assets

    def get_asset_data(self, path: str) -> bytes:
        """
        Return data for a non-collection asset at the given path

        :param path: asset path; must identify a non-collection asset

        :return: asset data
        """
        filename = self._path_to_filename(path)
        if not os.path.isfile(filename):
            raise AssetNotFoundError(path=path)

        # Return file contents
        try:
            if os.path.splitext(filename)[1] == '.gz':
                with gzip.GzipFile(filename, 'rb') as f:
                    return f.read()

            if os.path.splitext(filename)[1] == '.bz2':
                with bz2.BZ2File(filename, 'rb') as f:
                    return f.read()

            with open(filename, 'rb') as f:
                return f.read()
        except Exception as e:
            # noinspection PyUnresolvedReferences
            raise FilesystemError(reason=str(e))

    def create_asset(self, path: str, data: Optional[bytes] = None, **kwargs) \
            -> DataProviderAsset:
        """
        Create an asset at the given path

        :param path: path at which to create the asset
        :param data: FITS image data; if omitted, create a collection asset

        :return: new data provider asset object
        """
        # Check that the given path does not exist
        filename = self._path_to_filename(path)
        if os.path.exists(filename):
            raise AssetAlreadyExistsError()

        try:
            # Make sure that parent path exists
            d = os.path.dirname(filename)
            if not os.path.exists(d):
                try:
                    os.makedirs(d)
                except OSError as e:
                    if e.errno != EEXIST:
                        raise

            if data is None:
                # Create a collection asset
                os.makedirs(filename)
            else:
                # Save data to disk
                with open(filename, 'wb') as f:
                    f.write(data)

        except Exception as e:
            # noinspection PyUnresolvedReferences
            raise FilesystemError(reason=str(e))

        return self._get_asset(path, filename)

    def rename_asset(self, path: str, name: str, **kwargs) -> DataProviderAsset:
        """
        Rename asset at the given path

        :param path: path at which to create the asset
        :param name: new asset name

        :return: updated data provider asset object
        """
        # Check that the given path exists and is not a directory
        filename = self._path_to_filename(path)
        if not os.path.exists(filename):
            raise AssetNotFoundError(path=path)

        new_filename = os.path.join(
            os.path.dirname(filename), os.path.basename(name))
        if os.path.exists(new_filename):
            raise AssetAlreadyExistsError()

        # Rename file or directory
        try:
            os.rename(filename, new_filename)
        except Exception as e:
            raise FilesystemError(reason=str(e))

        return self._get_asset(
            new_filename.split(self.abs_root + os.path.sep)[1]
            .replace('\\', '/'), new_filename)

    def update_asset(self, path: str, data: Optional[bytes],
                     force: bool = False, **kwargs) \
            -> DataProviderAsset:
        """
        Update an asset at the given path

        :param path: path of the asset to update
        :param data: asset data; create non-collection asset if None
        :param force: recursively overwrite collection asset

        :return: updated data provider asset object
        """
        # Check that the given path exists and is not a directory
        filename = self._path_to_filename(path)
        if not os.path.exists(filename):
            raise AssetNotFoundError(path=path)
        if os.path.isdir(filename):
            if not force:
                raise CannotUpdateCollectionAssetError()
            try:
                shutil.rmtree(filename)
            except Exception as e:
                raise FilesystemError(reason=str(e))

        try:
            if data is None:
                # Create a collection asset
                if os.path.exists(filename):
                    # Overwriting a file with a directory, must delete it first
                    os.unlink(filename)
                os.makedirs(filename)
            else:
                # Save data to disk overwriting existing file
                with open(filename, 'wb') as f:
                    f.write(data)
        except Exception as e:
            raise FilesystemError(reason=str(e))

        return self._get_asset(path, filename)

    def delete_asset(self, path: str, **kwargs) -> None:
        """
        Delete an asset at the given path

        :param path: path of the asset to delete
        """
        # Check that the given path exists
        filename = self._path_to_filename(path)
        if not os.path.exists(filename):
            raise AssetNotFoundError(path=path)

        if os.path.isdir(filename):
            try:
                shutil.rmtree(filename)
            except Exception as e:
                # noinspection PyUnresolvedReferences
                raise FilesystemError(reason=str(e))
        else:
            try:
                os.remove(filename)
            except Exception as e:
                # noinspection PyUnresolvedReferences
                raise FilesystemError(reason=str(e))


class RestrictedRWLocalDiskDataProvider(LocalDiskDataProvider):
    """
    Local disk data provider with restricted write access
    """
    name = 'restricted_local_disk'
    display_name = description = 'Local Filesystem with Restricted Write Access'

    writers = ()

    def __getattribute__(self, item):
        if item == 'readonly':
            # Dynamic readonly attr implementation based on the currently
            # authenticated user's username
            return auth.current_user.username not in object.__getattribute__(
                self, 'writers')
        return super().__getattribute__(item)
