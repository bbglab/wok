from local import LocalPlatform
from cluster import ClusterPlatform

__PLATFORMS = {
	"local" : LocalPlatform,
	"cluster" : ClusterPlatform
}

def create_platform(name, conf):
	if name not in __PLATFORMS:
		return None

	conf["type"] = name

	return __PLATFORMS[name](conf)