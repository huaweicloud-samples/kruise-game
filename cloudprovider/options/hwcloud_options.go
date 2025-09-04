package options

type HwCloudOptions struct {
	Enable             bool               `toml:"enable"`
	ELBOptions         ELBOptions         `toml:"elb"`
	CCEELBOptions      CCEELBOptions      `toml:"cce-elb"`
	CCEMultiELBOptions CCEMultiELBOptions `toml:"cce-multi-elb"`
}

type CCEELBOptions struct {
	MaxPort    int32   `toml:"max_port"`
	MinPort    int32   `toml:"min_port"`
	BlockPorts []int32 `toml:"block_ports"`
}

func (e CCEELBOptions) valid(skipPortRangeCheck bool) bool {
	for _, blockPort := range e.BlockPorts {
		if blockPort >= e.MaxPort || blockPort <= e.MinPort {
			return false
		}
	}
	// old elb plugin only allow 200 ports.
	if !skipPortRangeCheck && int(e.MaxPort-e.MinPort)-len(e.BlockPorts) > 200 {
		return false
	}
	if !skipPortRangeCheck && (e.MinPort <= 0 || e.MaxPort > 65535) {
		return false
	}
	return true
}

type CCEMultiELBOptions struct {
	MaxPort    int32   `toml:"max_port"`
	MinPort    int32   `toml:"min_port"`
	BlockPorts []int32 `toml:"block_ports"`
}

func (e CCEMultiELBOptions) valid(skipPortRangeCheck bool) bool {
	for _, blockPort := range e.BlockPorts {
		if blockPort >= e.MaxPort || blockPort <= e.MinPort {
			return false
		}
	}
	// old elb plugin only allow 200 ports.
	if !skipPortRangeCheck && int(e.MaxPort-e.MinPort)-len(e.BlockPorts) > 200 {
		return false
	}
	if !skipPortRangeCheck && (e.MinPort <= 0 || e.MaxPort > 65535) {
		return false
	}
	return true
}

type ELBOptions struct {
	MaxPort    int32   `toml:"max_port"`
	MinPort    int32   `toml:"min_port"`
	BlockPorts []int32 `toml:"block_ports"`
}

func (e ELBOptions) valid(skipPortRangeCheck bool) bool {
	for _, blockPort := range e.BlockPorts {
		if blockPort >= e.MaxPort || blockPort <= e.MinPort {
			return false
		}
	}
	// old elb plugin only allow 200 ports.
	if !skipPortRangeCheck && int(e.MaxPort-e.MinPort)-len(e.BlockPorts) > 200 {
		return false
	}
	if !skipPortRangeCheck && (e.MinPort <= 0 || e.MaxPort > 65535) {
		return false
	}
	return true
}

func (o HwCloudOptions) Valid() bool {
	elbOptions := o.ELBOptions
	cceElbOptions := o.CCEELBOptions
	cceMltiElbOptions := o.CCEMultiELBOptions
	return elbOptions.valid(true) || cceElbOptions.valid(false) || cceMltiElbOptions.valid(false)
}

func (o HwCloudOptions) Enabled() bool {
	return o.Enable
}
