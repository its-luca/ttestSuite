package tPlot

import (
	"fmt"
	"golang.org/x/image/colornames"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"io"
	"math"
)

type sliceXY struct {
	yValues []float64
}

func (s *sliceXY) Len() int {
	return len(s.yValues)
}

func (s *sliceXY) XY(index int) (x, y float64) {
	x = float64(index)
	y = s.yValues[index]
	return
}

func maxFloat64(s []float64) (float64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("slice is empty")
	}
	max := s[0]
	for i := range s {
		if s[i] > max {
			max = s[i]
		}
	}
	return max, nil
}

//Plot creates a lines plot for the values in tValues with a horizontal line at threshold
func Plot(tValues []float64, threshold float64) (*plot.Plot, error) {
	p := plot.New()
	p.Title.Text = "T-Test"
	p.X.Label.Text = "Datapoint"
	p.Y.Label.Text = "T-Test Value"

	tPlot, err := plotter.NewLine(&sliceXY{tValues})
	if err != nil {
		return nil, fmt.Errorf("failed creating line for t values : %v\n", err)
	}
	tPlot.StepStyle = plotter.PreStep

	maxY, err := maxFloat64(tValues)
	if err != nil {
		return nil, fmt.Errorf("failed to determine max t value : %v", err)
	}

	thresholdLine := plotter.NewFunction(func(x float64) float64 {
		return threshold
	})

	thresholdLine.Color = colornames.Red

	p.Add(tPlot, thresholdLine)
	p.Legend.Add("T-Values", tPlot)
	p.Legend.Add("Threshold", thresholdLine)
	p.Legend.Top = true
	p.Y.Max = math.Max(maxY, threshold) + 2
	p.Y.Min = 0

	return p, nil

}

//PlotAndStore wraps Plot and stores the result in out
func PlotAndStore(tValues []float64, threshold float64, out io.Writer) error {
	p, err := Plot(tValues, threshold)
	if err != nil {
		return fmt.Errorf("failed to create plot :%v", err)
	}
	writerTo, err := p.WriterTo(800, 600, "png")
	if err != nil {
		return fmt.Errorf("failed to prepare plot for writing : %v", err)
	}
	if _, err := writerTo.WriteTo(out); err != nil {
		return fmt.Errorf("failed to write plot : %v", err)
	}
	return nil
}
