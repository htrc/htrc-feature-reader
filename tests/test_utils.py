import pytest
from htrc_features import utils
import os

@pytest.fixture(scope="module")
def volume_ids():
    return ["uc1.$b364513", "coo1.ark:/13960/t3dz0tr56",
            "ufl1.ark:/13960/t36120v0s", "hvd.32044100887389"]

@pytest.fixture(scope="module")
def pairtree_volume_paths():
    return ["uc1/pairtree_root/$b/36/45/13/$b364513/uc1.$b364513.json.bz2", "coo1/pairtree_root/ar/k+/=1/39/60/=t/3d/z0/tr/56/ark+=13960=t3dz0tr56/coo1.ark+=13960=t3dz0tr56.json.bz2", "ufl1/pairtree_root/ar/k+/=1/39/60/=t/36/12/0v/0s/ark+=13960=t36120v0s/ufl1.ark+=13960=t36120v0s.json.bz2", "hvd/pairtree_root/32/04/41/00/88/73/89/32044100887389/hvd.32044100887389.json.bz2"
    ]

@pytest.fixture(scope="module")
def stubbytree_volume_paths():
    return ["uc1/$61/uc1.$b364513.json.bz2", "coo1/a+30305/coo1.ark+=13960=t3dz0tr56.json.bz2", "ufl1/a+30320/ufl1.ark+=13960=t36120v0s.json.bz2", "hvd/34088/hvd.32044100887389.json.bz2"
    ]

class TestUtils():

    def test_file_available(self, volume_ids):
        fake_ids = ['fake_url1', 'fake_url2']
        idcheck = utils.files_available(fake_ids + volume_ids)
        for id in volume_ids:
            assert idcheck[id] is True
        for id in fake_ids:
            assert idcheck[id] is False

    def test_id_to_rsync(self, volume_ids, stubbytree_volume_paths, pairtree_volume_paths):
        for i, volume_id in enumerate(volume_ids):
            assert utils.id_to_rsync(volume_id, format='pairtree') == pairtree_volume_paths[i]
            assert utils.id_to_rsync(volume_id, format='stubbytree') == stubbytree_volume_paths[i]
            assert utils.id_to_rsync(volume_id) == stubbytree_volume_paths[i]

    def test_command_id(self, capsys, volume_ids, stubbytree_volume_paths, pairtree_volume_paths):
        ''' Test the command line tool for one id'''
        parser = utils._htid2rsync_argparser()
        for i, volume_id in enumerate(volume_ids):
            utils._htid2rsync_parse_args(parser, [volume_id])
            out, err = capsys.readouterr()
            assert out.strip() == stubbytree_volume_paths[i]

            utils._htid2rsync_parse_args(parser, ['--oldstyle', volume_id])
            out, err = capsys.readouterr()
            assert out.strip() == pairtree_volume_paths[i]
    
    def test_command_ids(self, capsys, volume_ids, stubbytree_volume_paths, pairtree_volume_paths):
        ''' Test the command line tool for multiple ids'''
        parser = utils._htid2rsync_argparser()
        
        utils._htid2rsync_parse_args(parser, volume_ids)
        out, err = capsys.readouterr()
        assert out.strip().split("\n") == stubbytree_volume_paths
        
        # Test oldstyle pairtree
        utils._htid2rsync_parse_args(parser, ['--oldstyle'] + volume_ids)
        out, err = capsys.readouterr()
        assert out.strip().split("\n") == pairtree_volume_paths
    
    def test_command_outfile(self, tmpdir, volume_ids, stubbytree_volume_paths, pairtree_volume_paths):
        ''' Test the command line tool with an output file'''
        # Output volume paths to file with outfile
        parser = utils._htid2rsync_argparser()
        outfile = os.path.join(str(tmpdir), "volumepaths.txt")
        utils._htid2rsync_parse_args(parser, ["--outfile", outfile] + volume_ids)
        
        # Save with short arg
        utils._htid2rsync_parse_args(parser, ["-o", outfile + ".short"] + volume_ids)
        
        # Try with pairtree
        utils._htid2rsync_parse_args(parser, ["--outfile", outfile + '.pairtree', '--oldstyle'] + volume_ids)
        
        # Re-open
        with open(outfile) as f:
            assert f.read().strip().split("\n") == stubbytree_volume_paths

        with open(outfile + ".short") as f:
            assert f.read().strip().split("\n") == stubbytree_volume_paths
            
        with open(outfile + ".pairtree") as f:
            assert f.read().strip().split("\n") == pairtree_volume_paths
    
    def test_command_infile(self, tmpdir, capsys, volume_ids, stubbytree_volume_paths):
        ''' Test the command line tool with an input file'''
        # Write input file for testing
        idfile = os.path.join(str(tmpdir), "idfile.txt")
        
        with open(idfile, "w") as f:
            f.write("\n".join(volume_ids))
        
        parser = utils._htid2rsync_argparser()
        
        # Assert proper functionality
        utils._htid2rsync_parse_args(parser, ["--from-file", idfile])
        out, err = capsys.readouterr()
        assert out.strip().split("\n") == stubbytree_volume_paths
        
        # Short arg
        utils._htid2rsync_parse_args(parser, ["--f", idfile])
        out2, err2 = capsys.readouterr()
        assert out == out2
        
        # Assert error when grouping cmd args and file
        with pytest.raises(SystemExit):
            utils._htid2rsync_parse_args(parser, ["--from-file", idfile] + volume_ids)
            
        # Assert error when grouping cmd args and file
        with pytest.raises(SystemExit):
            utils._htid2rsync_parse_args(parser, ["-f", idfile] + volume_ids)
            
    def test_rsync_single_file(self, tmpdir, volume_ids, pairtree_volume_paths):
        expected_fname = os.path.split(pairtree_volume_paths[0])[1]
        utils.download_file(htids=volume_ids[0], outdir=tmpdir.dirname, format='pairtree')
        assert os.path.exists(os.path.join(tmpdir.dirname, expected_fname))
        
    def test_rsync_multi_file(self, tmpdir, volume_ids, pairtree_volume_paths):
        utils.download_file(htids=volume_ids, outdir=tmpdir.dirname, format='pairtree')
        for path in pairtree_volume_paths:
            expected_fname = os.path.split(path)[1]
            assert os.path.exists(os.path.join(tmpdir.dirname, expected_fname))
            
    def test_recursive_rsync(self, tmpdir, volume_ids, pairtree_volume_paths):
        utils.download_file(htids=volume_ids, outdir=tmpdir.dirname, keep_dirs=True, format='pairtree')
        for path in pairtree_volume_paths:
            assert os.path.exists(os.path.join(tmpdir.dirname, path))