package ipip

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"github.com/mxlxm/ipip-go"
	//"strconv"
)

type IpipDecoderConfig struct {
	DatabaseFile  string `toml:"db_file"`
	SourceIpField string `toml:"source_ip_field"`
	TargetField   string `toml:"target_field"`
}

type IpipDecoder struct {
	DatabaseFile  string
	SourceIpField string
	TargetField   string
	gi            *ipip.Datx
	pConfig       *PipelineConfig
}

// Heka will call this before calling any other methods to give us access to
// the pipeline configuration.
func (ld *IpipDecoder) SetPipelineConfig(pConfig *PipelineConfig) {
	ld.pConfig = pConfig
}

func (ld *IpipDecoder) ConfigStruct() interface{} {
	globals := ld.pConfig.Globals
	return &IpipDecoderConfig{
		DatabaseFile:  globals.PrependShareDir("ipip.datx"),
		SourceIpField: "",
		TargetField:   "ipip",
	}
}

func (ld *IpipDecoder) Init(config interface{}) (err error) {
	conf := config.(*IpipDecoderConfig)

	if string(conf.SourceIpField) == "" {
		return errors.New("`source_ip_field` must be specified")
	}

	if conf.TargetField == "" {
		return errors.New("`target_field` must be specified")
	}

	ld.TargetField = conf.TargetField
	ld.SourceIpField = conf.SourceIpField

	if ld.gi == nil {
		ld.gi = ipip.Init(conf.DatabaseFile)
	}
	if err != nil {
		return fmt.Errorf("Could not open IPIP database: %s\n")
	}

	return
}

func (ld *IpipDecoder) GetRecord(ip string) *ipip.IPIP {
	t, _ := ld.gi.Find(ip)
	return t
}

func (ld *IpipDecoder) IpipBuff(rec *ipip.IPIP) bytes.Buffer {
	buf := bytes.Buffer{}

	buf.WriteString(`{`)

	buf.WriteString(`"lat":`)
	buf.WriteString(rec.LA)

	buf.WriteString(`,"lng":`)
	buf.WriteString(rec.LN)

	buf.WriteString(`,"location":[`)
	buf.WriteString(rec.LN)
	buf.WriteString(`,`)
	buf.WriteString(rec.LA)
	buf.WriteString(`]`)

	buf.WriteString(`,"countrycode":`)
	buf.WriteString(rec.CC)

	buf.WriteString(`,"country":"`)
	buf.WriteString(rec.CR)
	buf.WriteString(`"`)

	buf.WriteString(`,"region":"`)
	buf.WriteString(rec.RG)
	buf.WriteString(`"`)

	buf.WriteString(`,"city":"`)
	buf.WriteString(rec.CT)
	buf.WriteString(`"`)

	buf.WriteString(`,"isp":"`)
	buf.WriteString(rec.IS)
	buf.WriteString(`"`)

	buf.WriteString(`,"continentcode":`)
	buf.WriteString(rec.WC)

	buf.WriteString(`}`)

	return buf
}

func (ld *IpipDecoder) Decode(pack *PipelinePack) (packs []*PipelinePack, err error) {
	var buf bytes.Buffer
	var ipAddr, _ = pack.Message.GetFieldValue(ld.SourceIpField)

	ip, ok := ipAddr.(string)

	if !ok {
		// IP field was not a string. Field could just be blank. Return without error.
		packs = []*PipelinePack{pack}
		return
	}

	if ld.gi != nil {
		rec := ld.GetRecord(ip)
		if rec != nil {
			buf = ld.IpipBuff(rec)
		} else {
			// IP address did not return a valid ipip record. Return without error.
			packs = []*PipelinePack{pack}
			return
		}
	}

	if buf.Len() > 0 {
		var nf *message.Field
		nf, err = message.NewField(ld.TargetField, buf.Bytes(), "")
		pack.Message.AddField(nf)
	}

	packs = []*PipelinePack{pack}

	return
}

func init() {
	RegisterPlugin("IpipDecoder", func() interface{} {
		return new(IpipDecoder)
	})
}
