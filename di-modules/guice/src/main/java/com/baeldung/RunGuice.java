
package com.baeldung;

import com.baeldung.guice.Communication;
import com.baeldung.guice.binding.AOPModule;
import com.baeldung.guice.modules.BasicModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Scanner;

/**
 *
 * @author baeldung
 */
public class RunGuice {

    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new BasicModule(), new AOPModule());
        Communication comms = injector.getInstance(Communication.class);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String input = scanner.nextLine();
            if (input.equalsIgnoreCase("q")) {
                System.exit(0);
            } else {
                comms.sendMessage(input);
            }

        }

    }
}
