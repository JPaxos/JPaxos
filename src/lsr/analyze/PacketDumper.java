package lsr.analyze;

import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import lsr.paxos.messages.Message;
import lsr.paxos.messages.MessageFactory;

public class PacketDumper implements ActionListener, Runnable {

    private JTextField f;

    public static void main(String[] args) {
        SwingUtilities.invokeLater(new PacketDumper());
    }

    public void run() {
        JFrame q = new JFrame();
        q.setLayout(new GridLayout(1, 2));
        q.setTitle("MessageFactory.create() from TextFile");

        f = new JTextField();
        JButton b = new JButton("Ok");
        q.add(f);
        q.add(b);
        q.setSize(500, 70);

        b.setActionCommand("analyze");
        b.addActionListener(this);
        q.setVisible(true);
    }

    public void actionPerformed(ActionEvent e) {
        if (e.getActionCommand().equals("analyze")) {
            FileInputStream fis;
            try {
                fis = new FileInputStream(f.getText());
            } catch (FileNotFoundException e1) {
                JOptionPane.showMessageDialog(null, "File not found");
                System.exit(1);
                return;
            }
            DataInputStream dis = new DataInputStream(fis);
            JFrame j = new JFrame();
            j.setTitle(f.getText());
            JTextArea t = new JTextArea();
            t.setFont(new Font("Consolas", Font.PLAIN, 10));

            JProgressBar p;
            Message m;
            try {
                p = new JProgressBar(0, fis.available());
                new BoxLayout(j, 0);
                j.add(new JScrollPane(t));
                p.setSize(500, 100);
                p.setVisible(true);

                while (fis.available() != 0) {
                    p.setValue(fis.available());
                    int sender = dis.readInt();
                    m = MessageFactory.create(dis);
                    t.append(m.getSentTime() + String.format("| %3d: ", sender) + m.toString() +
                             "\n");
                }
            } catch (IOException e1) {
                JOptionPane.showMessageDialog(null, "IOError");
                System.exit(1);
                return;
            }

            p.setVisible(false);

            j.setSize(600, 800);
            j.setVisible(true);
        }
    }
}
