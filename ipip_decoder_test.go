package ipip

import (
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	ts "github.com/mozilla-services/heka/pipeline/testsupport"
	"github.com/mxlxm/ipip-go"
	"github.com/rafrombrc/gomock/gomock"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"testing"
)

func TestAllSpecs(t *testing.T) {
	r := gs.NewRunner()
	r.Parallel = false

	r.AddSpec(IpipDecoderSpec)

	gs.MainGoTest(r, t)
}

func IpipDecoderSpec(c gs.Context) {
	t := &ts.SimpleT{}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	pConfig := NewPipelineConfig(nil)
	pConfig.Globals.ShareDir = "/tmp"

	c.Specify("A IpipDecoder", func() {
		decoder := new(IpipDecoder)
		decoder.SetPipelineConfig(pConfig)
		rec := new(ipip.IPIP)
		conf := decoder.ConfigStruct().(*IpipDecoderConfig)

		c.Expect(conf.DatabaseFile, gs.Equals, "/tmp/ipip.datx")

		supply := make(chan *PipelinePack, 1)
		pack := NewPipelinePack(supply)

		nf, _ := message.NewField("remote_host", "74.125.142.147", "")
		pack.Message.AddField(nf)

		decoder.SourceIpField = "remote_host"
		conf.SourceIpField = "remote_host"
		decoder.Init(conf)

		rec.CC = "US"
		rec.CR = "United States"
		rec.RG = "CA"
		rec.CT = "Mountain View"
		rec.LA = "37.4192"
		rec.LN = "-122.0574"
		rec.WC = "NA"

		c.Specify("Test GeoIpDecoder Output", func() {
			buf := decoder.IpipBuff(rec)
			nf, _ = message.NewField("geoip", buf.Bytes(), "")
			pack.Message.AddField(nf)

			b, ok := pack.Message.GetFieldValue("geoip")
			c.Expect(ok, gs.IsTrue)

			c.Expect(string(b.([]byte)), gs.Equals, `{"lat":37.4192,"lng":-122.0574,"location":[-122.0574,37.4192],"countrycode":US,"country":"United States","region":"CA","city":"Mountain View","isp":"","continentcode":NA}`)
		})

	})
}
