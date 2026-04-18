package options

type HwCloudOptions struct {
	Enable          bool            `toml:"enable"`
	ELBOptions      ELBOptions      `toml:"elb"`
	CCEELBOptions   CCEELBOptions   `toml:"cce"`
	MultiELBOptions MultiElbOptions `toml:"multielb"`
}

type MultiElbOptions struct {
	MaxPort    int32   `toml:"max_port"`
	MinPort    int32   `toml:"min_port"`
	BlockPorts []int32 `toml:"block_ports"`
}

type CCEELBOptions struct {
	MaxPort    int32   `toml:"max_port"`
	MinPort    int32   `toml:"min_port"`
	BlockPorts []int32 `toml:"block_ports"`
}

type ELBOptions struct {
	MaxPort    int32   `toml:"max_port"`
	MinPort    int32   `toml:"min_port"`
	BlockPorts []int32 `toml:"block_ports"`
}

func (e HwCloudOptions) valid(skipPortRangeCheck bool) bool {
	elbOptions := e.ELBOptions
	cceElbOptions := e.CCEELBOptions
	multiELBOptions := e.MultiELBOptions
	for _, blockPort := range elbOptions.BlockPorts {
		if blockPort >= elbOptions.MaxPort || blockPort <= elbOptions.MinPort {
			return false
		}
	}
	if elbOptions.MinPort <= 0 || elbOptions.MaxPort > 65535 {
		return false
	}
	for _, blockPort := range cceElbOptions.BlockPorts {
		if blockPort >= cceElbOptions.MaxPort || blockPort <= cceElbOptions.MinPort {
			return false
		}
	}

	if cceElbOptions.MinPort <= 0 || cceElbOptions.MaxPort > 65535 {
		return false
	}
	for _, blockPort := range multiELBOptions.BlockPorts {
		if blockPort >= multiELBOptions.MaxPort || blockPort <= multiELBOptions.MinPort {
			return false
		}
	}

	if multiELBOptions.MinPort <= 0 || multiELBOptions.MaxPort > 65535 {
		return false
	}
	// old elb plugin only allow 200 ports.
	//if !skipPortRangeCheck && int(e.MaxPort-e.MinPort)-len(e.BlockPorts) > 200 {
	//	return false
	//}
	return true
}

func (o HwCloudOptions) Valid() bool {
	return o.valid(false)
}

func (o HwCloudOptions) Enabled() bool {
	return o.Enable
}
