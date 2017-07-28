

import javax.swing.*;
import java.awt.*;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.util.*;

public class DreamRunner {

    public DreamRunner()
    {
        windows=new JFrame();
        data = new Data();
        display = new Display(data);

        windows.setTitle("e2001527_Batyr_Charyyev");
        windows.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        windows.setSize(data.windowsWidth,data.windowsHeight);
        windows.add(display);
        windows.pack();
        windows.setVisible(true);

        resize_listen();

    }

    public void resize_listen()
    {
        windows.addComponentListener(new ComponentAdapter() {
            @Override
            public void componentResized(ComponentEvent e) {
                data.windowsWidth=windows.getWidth();
                data.windowsHeight=windows.getHeight();

                int w=data.windowsWidth/8;
                for(int i=0; i<data.courses.size(); i++)
                {
                    data.courses.get(i).x=i*w;
                    data.courses.get(i).width=w;
                }
                /////////////////////////////////////////////////////
                data.fairy.width=windows.getWidth();
                data.fairy.height=windows.getHeight();
            }
        });
    }

    public JFrame windows;

    public Display display;

    public Data data;

    public static void main(String[] args) throws InterruptedException {

        DreamRunner runner=new DreamRunner();

        while(true)
        {
            runner.data.stepAll();
            runner.windows.repaint();
            Thread.sleep(50L);
        }
    }
}
