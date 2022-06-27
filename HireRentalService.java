package com.compact.service.hire.implementation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Years;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.compact.client.CompactFactory;
import com.compact.database.hire.AccountingExportCustomer;
import com.compact.database.hire.BranchInvoice;
import com.compact.database.hire.Company;
import com.compact.database.hire.CompanyBranch;
import com.compact.database.hire.Customer;
import com.compact.database.hire.HireRights;
import com.compact.database.hire.ProcessActiveRate;
import com.compact.database.hire.ProcessAddress;
import com.compact.database.hire.ProcessCustomer;
import com.compact.database.hire.ProcessCustomerCard;
import com.compact.database.hire.ProcessDocument;
import com.compact.database.hire.ProcessPeriod;
import com.compact.database.hire.ProcessPricePositions;
import com.compact.database.hire.ProcessVehicle;
import com.compact.database.hire.Rental;
import com.compact.database.hire.RentalDate;
import com.compact.database.hire.RentalMiscellaneous;
import com.compact.database.hire.RentalPrice;
import com.compact.database.hire.Tax;
import com.compact.database.hire.User;
import com.compact.database.hire.Vehicle;
import com.compact.database.hire.VehicleMileage;
import com.compact.database.hire.ViewPlannerPosition;
import com.compact.database.hire.ViewProcessOverview;
import com.compact.database.hire.dao.AccountingDao;
import com.compact.database.hire.dao.CustomerDao;
import com.compact.database.hire.dao.HireRightsDao;
import com.compact.database.hire.dao.HireVehicleDao;
import com.compact.database.hire.dao.ProcessDocumentDao;
import com.compact.database.hire.dao.ProcessPeriodDao;
import com.compact.database.hire.dao.RentalDao;
import com.compact.database.hire.dao.TaxDao;
import com.compact.database.hire.dao.UserDao;
import com.compact.database.hire.dao.VehicleMileageDao;
import com.compact.database.hire.dao.ViewPlannerPositionDao;
import com.compact.database.hire.dao.ViewProcessOverviewDao;
import com.compact.enums.hire.DocumentType;
import com.compact.enums.hire.KMStatus;
import com.compact.enums.hire.ProcessPeriodStatus;
import com.compact.enums.hire.ProcessPricePositionTypes;
import com.compact.enums.hire.ProcessStatus;
import com.compact.enums.hire.ProcessTypes;
import com.compact.service.hire.interfaces.IHireCompanyService;
import com.compact.service.hire.interfaces.IHireCustomerService;
import com.compact.service.hire.interfaces.IHireProcessCustomerService;
import com.compact.service.hire.interfaces.IHireRentalService;
import com.compact.service.hire.interfaces.IHireVehicleService;
import com.compact.tools.hire.CarGuideException;
import com.compact.tools.hire.CarGuideProcessException;
import com.compact.tools.hire.Loggable;
import com.compact.tools.hire.RentalPricePosition;
import com.compact.tools.hire.ServerFileWriter;
import com.compact.tools.hire.exceptions.HIREProcessPeriodException;
import com.compact.tools.hire.exceptions.HIREVehicleException;
import com.compact.tools.hire.inspection.InspectRental;
import com.compact.tools.hire.invoicing.HIREInvoicingKM;
import com.compact.tools.hire.invoicing.VOLATaxCalculation;
import com.compact.tools.hire.period.HIREPeriod;
import com.compact.tools.hire.process.ProcessPeriodCalculator;
import com.compact.tools.hire.process.ProcessTimeCalculator;
import com.compact.transfer.hire.HireProcessSearchParams;
import com.compact.transfer.hire.HireTransferAccident;
import com.compact.transfer.hire.HireTransferContract;
import com.compact.transfer.hire.ProcessTime;
import com.compact.transfer.hire.ProcessVehicleSearchResult;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.versatio.tools.vola.comparator.ProcessPeriodComparator;
import com.versatio.transfer.vola.process.TransferProcessPeriod;

/**
 * Implementierung des Vorgangservice.
 */
@Component
@Transactional
public class HireRentalService extends Loggable implements IHireRentalService
{
    @Autowired
    private RentalDao rentalDao;
    @Autowired
    private ViewPlannerPositionDao viewPlannerPositionDao;
    @Autowired
    private CustomerDao customerDao;
    @Autowired
    HireRightsDao hireRightsDao;
    //    @Autowired
    //    private CompanyDao companyDao;
    //    @Autowired
    //    private CompanyBranchDao companyBranchDao;
    @Autowired
    private HireVehicleDao hireVehicleDao;
    @Autowired
    private UserDao hireUserDao;
    @Autowired
    private ViewProcessOverviewDao viewProcessOverviewDao;
    @Autowired
    private AccountingDao accountingDao;
    @Autowired
    private ProcessPeriodDao periodDao;
    @Autowired
    private VehicleMileageDao vehicleMileageDao;
    @Autowired
    private ProcessDocumentDao documentDao;
    @Autowired
    private TaxDao taxDao;

    /**
     * document output path
     */
    private String documentOutputPath_ = "/var/spool/hire/";
    //private String documentOutputPath_ = "C:\\Users\\mburbach\\Documents";

    /**
     * Überprüft und erstellt neue Vorgänge
     */
    @Override
    public Integer addRentals( Rental rental ) throws CarGuideException
    {
        this.getCurrentSession();

        if( !InspectRental.inspectNewRental( rental, this.getHireUser() ) ) {
            return null;
        }

        rental.getRentalDate().setDrivingEndTime( rental.getRentalDate().getEndTime() );
        ProcessVehicle processVehicle = rental.getProcessVehicle();
        Vehicle vehicle = processVehicle.getVehicle();
        processVehicle.setVin( vehicle.getVin() );
        processVehicle.setType( vehicle.getType() );
        processVehicle.setRegistrationNumber( vehicle.getDetail().getRegistrationNumber() );
        processVehicle.setBrand( vehicle.getVehicleCode().getName() );

        //Ermittlung Vorgangstype
        //TODO C: double implementation in inspectNewRental
        if( !rental.getType().equals( ProcessTypes.FINANCIAL.getValue() ) ) {
            if( rental.getRate() != null && !rental.getType().equals( ProcessTypes.TRANSPORTATION.getValue() ) ) {
                if( rental.getRate().getType().equals( "TRIAL" ) ) {
                    rental.setType( ProcessTypes.TRIAL.getValue() );
                }
                else {
                    rental.setType( ProcessTypes.RENTAL.getValue() );
                }
            }
            else {
                rental.getProcessFlags().setDevaiteDriver( true );
            }
        }

        User lastChangedUser = hireUserDao.getUserById( getHireUser().getUserId() );
        rental.getUsers().setCreatorUser( lastChangedUser );
        rental.setLastChangeUser( lastChangedUser );
        rental.getRentalDate().setLastChangeTime( new Date() );
        rental.setStatus( ProcessStatus.RESERVED.getValue() );
        //Führerschein gesichtet?
        if( rental.getProcessFlags().isDriverLicenseInspection() ) {
            rental.getRentalDate().setDriverLicenseInspectionTime( new Date() );
            rental.getUsers().setDriverLicenseUser( lastChangedUser );
        }

        if( rental.getType().equals( ProcessTypes.RENTAL.getValue() ) ) {
            /*
             * check if ProcessCustomer has a customer number, if not genrate
             * new number This process is only possible if customers are the
             * same in all branches (Nagel Rent)
             */
            boolean allowSettingCustomerNo = rental.getCompany().isHireDebitor();
            if( allowSettingCustomerNo ) {
                this.setCustomerNoForProcessCustomer( rental.getCustomer(), rental );
            }
            if( rental.getCustomer().getCustomerType().equals( Integer.valueOf( 1 ) ) ) {
                log( "Remove first and lastname from customer" );
                rental.getCustomer().setFirstname( null );
                rental.getCustomer().setLastname( null );
            }
            else {
                log( "Remove company name and department from customer" );
                rental.getCustomer().setCompanyName( null );
                rental.getCustomer().setDepartment( null );
            }

            //secure setting: creates driver if not exist
            if( !rental.getProcessFlags().isDevaiteDriver() ) {
                ProcessCustomer.overwriteProcessCustomerToDriver( rental.getCustomer(), rental.getDriver() );
            }
            //secure setting: creates invoice recipient if not exist
            if( !rental.getProcessFlags().isDevaiteInvoiceRecipient() ) {
                ProcessCustomer.overwriteProcessCustomerToRecipient( rental.getCustomer(), rental.getInvoiceRecipient() );
            }
            else {
                if( allowSettingCustomerNo ) {
                    this.setCustomerNoForProcessCustomer( rental.getInvoiceRecipient(), rental );
                }
                if( rental.getInvoiceRecipient().getCustomerType().equals( Integer.valueOf( 1 ) ) ) {
                    log( "Remove first and lastname from recipient" );
                    rental.getInvoiceRecipient().setFirstname( null );
                    rental.getInvoiceRecipient().setLastname( null );
                }
                else {
                    log( "Remove company name and department from recipient" );
                    rental.getInvoiceRecipient().setCompanyName( null );
                    rental.getInvoiceRecipient().setDepartment( null );
                }
            }
        }

        log( rental.getCustomer() );
        log( rental.getDriver() );
        log( rental.getInvoiceRecipient() );

        //Setzen Datenbank Beziehungen
        rental.getCustomer().setType( "CUSTOMER" );
        rental.getDriver().setType( "DRIVER" );
        if( rental.getInvoiceRecipient() != null ) {
            rental.getInvoiceRecipient().setType( "RECIPIENT" );
        }

        rental.getRate().setRental( rental );
        try {
            log( "Rental Object wird nun versucht zu speichern." );
            rental = rentalDao.saveRental( rental );
            if( rental != null ) {

                BigDecimal startKm = rental.getMisc().getStartKm();
                VehicleMileage mileage = new VehicleMileage( vehicle, rental, startKm, new Date(), this.getHireUser().getUserId(),
                        "SYSTEM: NEW RENTAL" );
                vehicleMileageDao.saveMileage( mileage );
                log( "Saved mileage (new rental) for vehicle id {" + vehicle.getId() + "}: " + mileage.getKilometers() );

                //sendStatusMail( rental, rental.getBranch().getId() );

                return rental.getId();
            }
            return null;
        }
        catch( Exception e ) {
            e.printStackTrace();
            throw new CarGuideException( "Fehler beim Erstellen des Termines: " + e.toString(), 999, 0 );
        }
    }

    /**
     * Get procesw object, for a valid id. Note: at moment, fallback is active
     */
    @Override
    public Rental getRentalById( Integer rentalId ) throws CarGuideException
    {
        this.getCurrentSession();
        if( rentalId == null || rentalId.intValue() < 1 ) {
            throw new CarGuideException( "rentalId ist null", 901, 0 );
        }
        Rental rental = rentalDao.getRentalById( rentalId );
        //fallbackForPeriodes( rental );
        return rental;
    }

    @Override
    public boolean updateBookedRental( Integer id, boolean isBooked ) throws CarGuideException
    {
        this.getCurrentSession();
        if( id == null || id.intValue() < 1 ) {
            throw new CarGuideException( "Keine gültige Fahrzeug Id", 999, 0 );
        }
        if( !getHireUser().isCompact() ) {
            throw new CarGuideException( "Nicht genügend Rechte!", 20, 21 );
        }
        Rental process = getRentalById( id );
        if( process != null ) {
            process.getRentalDate().setLastChangeTime( new Date() );
            User user = hireUserDao.getUserById( getHireUser().getUserId() );
            process.setLastChangeUser( user );
            process.getProcessFlags().setBooked( isBooked );
            log( "Setzt Buchnungsflag für Vorgang " + id + " auf " + isBooked );
            return rentalDao.updateRental( process );
        }
        log( "Vorgang mit Id " + id + " nicht gefunden." );
        return false;
    }

    private BigDecimal getLatestVehicleMileage( final Vehicle vehicle, Integer rentalId ) throws CarGuideException
    {
        if( vehicle == null ) {
            throw HIREVehicleException.VEHICLE_NULL;
        }
        Integer vehicleId = vehicle.getId();
        if( vehicleId == null ) {
            throw HIREVehicleException.NO_VEHICLE;
        }
        VehicleMileage mileage = vehicleMileageDao.getLatestMileageForVehicleId( vehicleId, rentalId );
        if( mileage == null ) {
            return new BigDecimal( 0.0 ); //throw HIREVehicleException.NO_VEHICLE_MILEAGE;
        }
        BigDecimal ret = mileage.getKilometers();

        if( ret.doubleValue() < 0.0 ) {
            throw HIREVehicleException.INVALID_VEHICLE_MILEAGE;
        }
        return ret;
    }

    private BigDecimal getHighestVehicleMileage( final Vehicle vehicle ) throws CarGuideException
    {
        if( vehicle == null ) {
            throw HIREVehicleException.VEHICLE_NULL;
        }
        Integer vehicleId = vehicle.getId();
        if( vehicleId == null ) {
            throw HIREVehicleException.NO_VEHICLE;
        }
        VehicleMileage mileage = vehicleMileageDao.getHighestMileageForVehicleId( vehicleId );
        if( mileage == null ) {
            return new BigDecimal( 0.0 ); //throw HIREVehicleException.NO_VEHICLE_MILEAGE;
        }
        BigDecimal ret = mileage.getKilometers();

        if( ret.doubleValue() < 0.0 ) {
            throw HIREVehicleException.INVALID_VEHICLE_MILEAGE;
        }
        return ret;
    }

    @Override
    public boolean updateRental( Rental rental ) throws CarGuideException
    {
        try {
            this.getCurrentSession();

            User lastChangedUser = ( User ) hireUserDao.getUserById( getHireUser().getUserId() );
            boolean vehicleUpdatet = false;
            Vehicle vehicle = rental.getProcessVehicle().getVehicle(); //vehicleService.getVehicleById( rental.getProcessVehicle().getVehicle().getId() );
            if( rental.getCustomer() != null ) {
                rental.getCustomer().setType( "CUSTOMER" );
                if( rental.getCustomer().getCustomerType().equals( Integer.valueOf( 1 ) ) ) {
                    log( "Remove first and lastname from customer" );
                    rental.getCustomer().setFirstname( null );
                    rental.getCustomer().setLastname( null );
                }
                else {
                    log( "Remove company name and department from customer" );
                    rental.getCustomer().setCompanyName( null );
                    rental.getCustomer().setDepartment( null );
                }
            }
            else {
                log( "Customer null. Vorgang:" + rental.getId() );
            }
            if( rental.getDriver() != null ) {
                rental.getDriver().setType( "DRIVER" );
            }
            else {
                log( "Driver null. Vorgang:" + rental.getId() );
            }
            if( rental.getInvoiceRecipient() != null ) {
                if( rental.getInvoiceRecipient().getCustomerType().equals( Integer.valueOf( 1 ) ) ) {
                    log( "Remove first and lastname from recipient" );
                    rental.getInvoiceRecipient().setFirstname( null );
                    rental.getInvoiceRecipient().setLastname( null );
                }
                else {
                    log( "Remove company name and department from recipient" );
                    rental.getInvoiceRecipient().setCompanyName( null );
                    rental.getInvoiceRecipient().setDepartment( null );
                }
            }
            if( !getHireUser().getUserId().equals( rental.getUser().getId() )
                    && !getHireUser().hasRight( HireRights.VERMITTLER, rental.getBranch() ) ) {
                throw new CarGuideException( "Nicht genügend Rechte", 20, 12 );
            }
            IHireRentalService rentalService = CompactFactory.createService( IHireRentalService.class );
            final Rental oldProcess = rentalService.getRentalById( rental.getId() );

            if( rental.getProcessVehicle().getVehicle() == null || rental.getProcessVehicle().getVehicle().getId() == null ) {
                throw new CarGuideProcessException( "Kein Fahrzeug ausgewählt.", 8, 12, true );
            }
            if( rental.getMisc().isWithWinterTires() != vehicle.getStatus().isWithWinterTires() ) {
                vehicle.getStatus().setWithWinterTires( rental.getMisc().isWithWinterTires() );
                vehicleUpdatet = true;
            }

            //Änderungen die nur mit Status 1 erlaubt sind
            if( rental.getStatus().equals( ProcessStatus.RESERVED.getValue() ) ) {
                if( !oldProcess.getProcessFlags().isDriverLicenseInspection() && rental.getProcessFlags().isDriverLicenseInspection() ) {
                    rental.getRentalDate().setDriverLicenseInspectionTime( new Date() );
                    rental.getUsers().setDriverLicenseUser( lastChangedUser );
                    log( "Set Driverlicense for Rental" + rental.getId() );
                }
                boolean appointmentChanged = false;
                if( !rental.getRentalDate().getEndTime().equals( oldProcess.getRentalDate().getEndTime() ) ) {
                    appointmentChanged = true;
                    log( "Endzeit für Vorgang " + rental.getId() + " geändert." );
                    if( rental.getType().intValue() == 3 ) {
                        rental.getRentalDate().setDrivingEndTime( rental.getRentalDate().getEndTime() );
                        log( "End Fahrzeit für Vorgang " + rental.getId() + " geändert." );
                    }
                }
                if( !rental.getRentalDate().getStartTime().equals( oldProcess.getRentalDate().getStartTime() ) ) {
                    appointmentChanged = true;
                    log( "Startzeit für Vorgang " + rental.getId() + " geändert." );
                }
                if( appointmentChanged ) {
                    rental.getRentalDate().setDrivingEndTime( rental.getRentalDate().getEndTime() );
                }
                //Überprüfe ob für das Fahrzeug
                RentalDate dates = rental.getRentalDate();
                InspectRental.checkForAnotherProcess( rental, false );
            }
            //Überprüfe für Vorgänge mit Status RESERVED, DRIVING oder BACK und Mietfahrt
            if( rental.getType().equals( ProcessTypes.RENTAL.getValue() )
                    && rental.getStatus().intValue() <= ProcessStatus.BACK.getValue() ) {
                /*
                 * check if ProcessCustomer has a customer number, if not
                 * genrate new number This process is only possible if customers
                 * are the same in all branches (Nagel Rent)
                 */
                boolean allowSettingCustomerNo = rental.getCompany().isHireDebitor();
                if( allowSettingCustomerNo ) {
                    this.setCustomerNoForProcessCustomer( rental.getCustomer(), rental );
                }
                //secure setting
                if( !rental.getProcessFlags().isDevaiteInvoiceRecipient() ) {
                    ProcessCustomer.overwriteProcessCustomerToRecipient( rental.getCustomer(), rental.getInvoiceRecipient() );
                }
                else {
                    if( allowSettingCustomerNo ) {
                        this.setCustomerNoForProcessCustomer( rental.getInvoiceRecipient(), rental );
                    }
                }
                //secure setting
                if( !rental.getProcessFlags().isDevaiteDriver() && rental.getStatus().intValue() == ProcessStatus.RESERVED.getValue() ) {
                    ProcessCustomer.overwriteProcessCustomerToDriver( rental.getCustomer(), rental.getDriver() );
                }
            }

            if( rental.getRate().isHasFixPrice() && rental.getRate().getFixPrice().compareTo( new BigDecimal( 0 ) ) <= 0
                    && !this.getHireUser().hasRight( HireRights.FILIALLEITER, rental ) ) {
                throw new CarGuideProcessException( "Fixpreis muss über 0 € liegen.", 9, 48, true );
            }

            //Bei Wechsel von Status 2 auf 3
            if( rental.getStatus().intValue() == ProcessStatus.DRIVING.getValue() ) {
                Date startTime = rental.getRentalDate().getStartTime();
                DateTime threeYearsStartTime = new DateTime( startTime ).plus( Years.THREE );

                //updatet ProcessPeriods for ReturnTime
                try {
                    Set < TransferProcessPeriod > transferPeriods = ProcessPeriod
                            .convertToTransferProcessPeriod( rental.getProcessPeriods() );
                    ProcessPeriodCalculator periodClaculator = new ProcessPeriodCalculator(
                            ProcessStatus.getStatusFromInt( rental.getStatus() ), startTime, rental.getRentalDate().getCurrentReturnTime(),
                            rental.getRate().getUnit(), rental.getRate().getPeriodStartDate(), transferPeriods );
                    transferPeriods = periodClaculator.updateExistingAccountingPeriodsForProcess();
                    rental.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, rental ) );
                }
                catch( HIREProcessPeriodException e ) {
                    log( e.getMessage() );
                }

                if( rental.getRentalDate().getReturnTime() != null ) {
                    if( rental.getRentalDate().getReturnTime().compareTo( threeYearsStartTime.toDate() ) > 0 ) {
                        throw new CarGuideProcessException( "Ankunftzeit ist zu groß.", 9, 64, true );
                    }
                    if( rental.getRentalDate().getReturnTime().compareTo( startTime ) < 0 ) {
                        throw new CarGuideProcessException( "Ankunftzeit liegt vor Abfahrtzeit.", 9, 64, true );
                    }

                    if( !rental.getLocations().getDestinationAddress().getCity().trim().isEmpty() && rental.getMisc().getEndKm() != null
                            && rental.getMisc().getEndKm().compareTo( rental.getMisc().getStartKm() ) == 1 ) {
                        log( "Ändere Status von 2 auf 3 für Vorgang " + rental.getId() );

                        //Anpassung KM-Stand des Fahrzeugs
                        log( "Registriere Km-Stand (Rücknahme) für Fahrzeug: " + vehicle.getVin() );
                        VehicleMileage vmileage = new VehicleMileage( vehicle, rental, rental.getMisc().getEndKm(), new Date(),
                                this.getHireUser().getUserId(), "SYSTEM: RENTAL RETURN" );
                        vehicleMileageDao.prepare( vmileage );

                        if( rental.getLocations().getDestinationBranch() != null ) {
                            vehicle.setBranch( rental.getLocations().getDestinationBranch() );
                            log( "Ändere Branch für Vehicle: " + vehicle.getVin() );
                            vehicleUpdatet = true;
                        }
                        if( rental.getCheckList().getDamageIn() != null && !rental.getCheckList().getDamageIn().trim().isEmpty() ) {
                            vehicle.getDamage().setDescription( rental.getCheckList().getDamageIn() );
                            log( "Ändere Schaden für Vehicle: " + vehicle.getVin() );
                            vehicleUpdatet = true;
                        }

                        //vehicleService.updateVehicle( vehicle );
                        //Setze den neuen Status
                        rental.setStatus( ProcessStatus.BACK.getValue() );
                        //sendStatusMail( rental, rental.getBranch().getId() );
                    }
                    else if( rental.getLocations().getDestinationAddress().getCity().trim().isEmpty()
                            && ( rental.getRentalDate().getReturnTime() != null || rental.getMisc().getEndKm() != null ) ) {
                        throw new CarGuideProcessException( "Ankunftsort ist nicht eingetragen.", 9, 64, true );
                    }
                    else if( rental.getMisc().getEndKm() == null && rental.getRentalDate().getReturnTime() != null ) {
                        throw new CarGuideProcessException( "Kein KM Stand eingetragen.", 9, 64, true );
                    }
                    else if( rental.getMisc().getEndKm() != null
                            && rental.getMisc().getEndKm().compareTo( rental.getMisc().getStartKm() ) < 1 ) {
                        throw new CarGuideProcessException( "Kilometer Ankunft ist kleiner/gleich Abfahrt Kilometer.", 9, 64, true );
                    }
                }
            }
            if( rental.getStatus() < ProcessStatus.BACK.getValue() ) {
                //KM-Stand des Fahrzeugs anpassen
                BigDecimal latestMileage = getLatestVehicleMileage( vehicle, rental.getId() );
                BigDecimal highestMileage = getHighestVehicleMileage( vehicle );
                BigDecimal startKm = rental.getMisc().getStartKm();
                log( ">>> latestMileage = " + highestMileage.doubleValue() + ">>> latestMileage = " + latestMileage.doubleValue()
                        + " Rental Start-Km = " + startKm.doubleValue() + " for rental {" + rental.getId() + "}" );
                log( "Vorgang {" + rental.getId() + "}: Registriere Km-Stand " + startKm.doubleValue() + " für Fahrzeug: "
                        + vehicle.getVin() );
                VehicleMileage vmileage = new VehicleMileage( vehicle, rental, startKm, new Date(), this.getHireUser().getUserId(),
                        "SYSTEM: RENTAL START UPDATE " + rental.getStatus() );
                vehicleMileageDao.prepare( vmileage );
            }

            rental.getRentalDate().setLastChangeTime( new Date() );

            rental.setLastChangeUser( ( User ) lastChangedUser.clone() );
            //oldProcess = null;
            if( vehicleUpdatet ) {
                CompactFactory.createService( IHireVehicleService.class ).updateVehicle( vehicle );
            }

            if( !rentalDao.updateRental( rental ) ) {
                return false;
            }
            if( !vehicleMileageDao.flush() ) {
                log( "Error updating mileage list for vehicle id {" + vehicle.getId() + "} !" );
                return false;
            }
            return true;
        }
        catch( CarGuideProcessException e ) {
            throw e;
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Adds an entry for given rental and documentType to the table of process
     * documents
     * 
     * @param rental
     * @param documentType
     *            a value of INVOICE, CONTRACT, INTERNAL_DRIVE, or
     *            SUBROGATION_AGREEMENT
     * @param documentNumber
     * @param price
     * @param period
     * @param documentDate
     * @return
     * @throws CarGuideException
     */
    private boolean createProcessDocuments( Rental rental, DocumentType documentType, final String documentNumber, final RentalPrice price,
            final ProcessPeriod period, final Date documentDate ) throws CarGuideException
    {
        Set < ProcessDocument > documents = rental.getProcessDocuments();
        log( "++++ Rental = " + rental.getId() + " Process = " + rental.getProcessNumber() + " document list size = " + documents.size() );

        User user = hireUserDao.getUserById( getHireUser().getUserId() );
        ProcessDocument processDocument = new ProcessDocument( user, rental, documentNumber, documentDate, documentType.toString(), price );
        if( period != null ) {
            processDocument.getPeriods().add( period );
            period.getProcessDocuments().add( processDocument );
            if( documentType == DocumentType.INVOICE ) {
                period.setStatus( ProcessPeriodStatus.INVOICE.getValue() );
            }
            else if( documentType == DocumentType.CREDIT ) {
                period.setStatus( ProcessPeriodStatus.VOUCHER.getValue() );
            }
            period.setLastUpdate( new Date() );
        }
        if( price != null ) {
            price.setProcessDocument( processDocument );
        }

        if( documents.add( processDocument ) ) {
            return true;
        }
        log( "Error while adding to process documents" );
        return false;
    }

    private boolean addAccountingExport( RentalPrice price )
    {
        if( price == null ) {
            log( "Error: Price is null" );
            return false;
        }

        if( price.getAccountingExportCustomers().size() > 0 ) {
            log( "Accounting Export User already added" );
            return true;
        }
        AccountingExportCustomer exportCustomer = new AccountingExportCustomer( price );
        ProcessDocument document = price.getProcessDocument();

        if( document == null ) {
            log( "Error: Process document is null" );
            return false;
        }

        Rental rental = document.getRental();

        if( rental == null ) {
            log( "Error: Rental is null" );
            return false;
        }

        ProcessCustomer invoiceRecipient = rental.getInvoiceRecipient();

        if( invoiceRecipient == null ) {
            log( "Error: Invoice recipient not set" );
            return false;
        }

        ProcessAddress address = invoiceRecipient.getProcessAddress();

        if( address == null ) {
            log( "Error: Address is null" );
            return false;
        }

        try {
            InspectRental.checkProcessCustomers( rental );
        }
        catch( CarGuideException e ) {
            log( "Error: Invalid process customer data" );
            log( e.getMessage() );
            return false;
        }
        exportCustomer.setCustomerno( invoiceRecipient.getCustomerNo() );
        exportCustomer.setFirstname( invoiceRecipient.getFirstname() );
        exportCustomer.setLastname( invoiceRecipient.getLastname() );
        exportCustomer.setCompanyName( invoiceRecipient.getCompanyName() );
        exportCustomer.setDepartment( invoiceRecipient.getDepartment() );
        String streetHouseNoPostbox = "";
        if( address.getStreet() != null && !address.getStreet().trim().isEmpty() ) {
            streetHouseNoPostbox += address.getStreet().trim();
        }
        if( address.getHouseno() != null && !address.getHouseno().trim().isEmpty() ) {
            if( !streetHouseNoPostbox.isEmpty() ) {
                streetHouseNoPostbox += " ";
            }
            streetHouseNoPostbox += address.getHouseno().trim();
        }
        if( address.getPostboxno() != null && !address.getPostboxno().trim().isEmpty() ) {
            streetHouseNoPostbox = "Postfach " + address.getPostboxno().trim();
        }
        exportCustomer.setStreetHousenoPostbox( streetHouseNoPostbox );
        exportCustomer.setZipcode( address.getZipcode() );
        exportCustomer.setCity( address.getCity() );
        exportCustomer.setState( address.getState() );
        exportCustomer.setDate( document.getDocumentDate() );

        Set < AccountingExportCustomer > accountingExportCustomers = price.getAccountingExportCustomers();
        if( accountingExportCustomers == null ) {
            log( "Error: Set of accounting export customers is null" );
            return false;
        }

        if( !accountingExportCustomers.add( exportCustomer ) ) {
            log( "Export customer already exists" );
        }

        return true;
    }

    /**
     * Generate Contract or Invoice JSON from Rental Object For rental status >=
     * {@link ProcessStatus#BACK} JSON for second and last Contract is
     * generated. Otherwise first contract.
     * 
     * @param rental
     * @param outputType
     *            can be INVOICE, CONTRACT, or INTERNAL_DRIVE
     * @param documentNumber
     * @param price
     * @param period
     * @param documentDate
     * @return
     * @throws CarGuideException
     */
    public boolean writeJSONDocumentFromRental( Rental rental, DocumentType outputType, final String documentNumber,
            final RentalPrice price, final ProcessPeriod period, final Date documentDate ) throws CarGuideException
    {
        Gson gson = new GsonBuilder()
                ////.registerTypeAdapter(Id.class, new IdTypeAdapter())
                ////.setExclusionStrategies( strategies )
                .enableComplexMapKeySerialization().serializeNulls().setDateFormat( "yyyy-MM-dd HH:mm" )
                .setFieldNamingPolicy( FieldNamingPolicy.IDENTITY ).setPrettyPrinting().setVersion( 1.0 ).create();

        List < RentalPricePosition > viewablePositions = new ArrayList < RentalPricePosition >();
        for( ProcessPricePositions position : price.getProcessPricePositions() ) {
            viewablePositions.add( position.toProcessPricePosition() );
        }

        String type = null;
        if( outputType == DocumentType.INTERNAL_DRIVE ) {
            if( rental.getStatus() >= ProcessStatus.BACK.getValue() ) {
                type = "internal_last_";
            }
            else {
                type = "internal_first_";
            }
        }
        if( outputType == DocumentType.CONTRACT ) {
            if( rental.getStatus() >= ProcessStatus.BACK.getValue() ) {
                type = "second_contract_";
            }
            else {
                type = "first_contract_";
            }
        }
        String pathNameJson = type + documentNumber.trim();
        //TODO: prevent overwriting contract
        if( outputType == DocumentType.INVOICE && rental.getStatus() >= ProcessStatus.DRIVING.getValue()
                && !documentNumber.trim().isEmpty() ) {
            pathNameJson = "rechnung_" + documentNumber.trim();
        }
        if( outputType == DocumentType.CREDIT && rental.getStatus() >= ProcessStatus.DRIVING.getValue()
                && !documentNumber.trim().isEmpty() ) {
            pathNameJson = "gutschrift_" + documentNumber.trim();
        }

        if( price != null && outputType != DocumentType.CONTRACT && !addAccountingExport( price ) ) {
            log( "Error while creating accounting export" );
            return false;
        }
        HireTransferContract transfer = new HireTransferContract();
        log( "Calling HireTransferContract.fromRental" );
        transfer.fromRental( rental, documentNumber, price, period, viewablePositions, documentDate, outputType );
        String documentOutput = gson.toJson( transfer );

        if( documentOutput == null || documentOutput.isEmpty() ) {
            throw new CarGuideException( "JSON Export fehlgeschlagen", 998, 0 );
        }

        //Call only if AccountingExportCustomer created
        ServerFileWriter.writeJSONToFileSystem( pathNameJson, documentOutputPath_, documentOutput );

        return true;
    }

    /**
     * Generate JSON for SubrogationAgreement
     * 
     * @param rental
     * @throws CarGuideException
     */
    public void writeJSONSubrogationAgreement( Rental rental ) throws CarGuideException
    {
        Gson gson = new GsonBuilder()
                ////.registerTypeAdapter(Id.class, new IdTypeAdapter())
                ////.setExclusionStrategies( strategies )
                .enableComplexMapKeySerialization().serializeNulls().setDateFormat( "yyyy-MM-dd HH:mm" )
                .setFieldNamingPolicy( FieldNamingPolicy.IDENTITY ).setPrettyPrinting().setVersion( 1.0 ).create();

        HireTransferAccident transfer = new HireTransferAccident();
        log( "Calling HireTransferAccident.fromRental" );
        transfer.fromRental( rental, rental.getProcessNumber() );
        String documentOutput = gson.toJson( transfer );
        log( "Generated JSON Output" );

        if( documentOutput == null || documentOutput.isEmpty() ) {
            throw new CarGuideException( "JSON Export fehlgeschlagen", 998, 0 );
        }

        String pathNameJson = "abtretung_" + rental.getProcessNumber();
        log( "writeJSONSubrogationAgreement: " + pathNameJson );
        ServerFileWriter.writeJSONToFileSystem( pathNameJson, documentOutputPath_, documentOutput );

        //TODO A: needs own method to save SUBROGATION_AGREEMENT to processDocument          
        createProcessDocuments( rental, DocumentType.SUBROGATION_AGREEMENT, rental.getProcessNumber(), null, null, new Date() );

    }

    @Override
    public boolean printContract( Integer rentalId ) throws CarGuideException
    {
        try {
            this.getCurrentSession();
            if( rentalId == null || rentalId.intValue() <= 0 ) {
                throw new CarGuideException( "Id ist null", 901, 0 );
            }
            Rental rental = rentalDao.getRentalById( rentalId );
            if( rental == null ) {
                throw new CarGuideException( "Keine Vermietung gefunden.", 9, 21 );
            }
            
            boolean finance = false;
            if( rental.getType() == 4 ) {
                finance = true;
            }
            
            if( !getHireUser().getUserId().equals( rental.getUser().getId() )
                    && !getHireUser().hasRight( HireRights.VERMITTLER, rental.getBranch() ) ) {
                throw new CarGuideException( "Nicht genügend Rechte.", 20, 12 );
            }
            //TODO C: Not needed anymore, maybe later need again
            boolean printGTC = false;
            boolean printWinterTyreDocument = false;

            if( rental.getStatus().intValue() == ProcessStatus.RESERVED.getValue() ) {
                printGTC = true;
                /*
                 * TODO A: in Werte einstallbar für jede Filiale/Firma in
                 * Datenbank speichern, wenn Hire eine längerfristige Zukunft
                 * hat. Alternativ mit GUI Popup verbinden, so das ab September
                 * bis Ende April der Benutzer gefragt wird ob Winter Reifen
                 * Dokument und AGBs beim Ausdruck vorhanden sein sollen.
                 */
                if( rental.getCompany().getId().equals( Integer.valueOf( 1 ) ) ) {
                    DateTime currentDateTime = new DateTime();
                    DateTime winterStartTime = new DateTime( currentDateTime.getYear(), 10, 1, 0, 0, 0, 0 );
                    DateTime winterEndTime = new DateTime( currentDateTime.plus( Years.ONE ).getYear(), 3, 31, 23, 59, 59, 0 );
                    if( currentDateTime.compareTo( winterStartTime ) > 0 && currentDateTime.compareTo( winterEndTime ) < 0 ) {
                        printWinterTyreDocument = true;
                    }
                }
                if( ( !rental.getType().equals( ProcessTypes.TRANSPORTATION.getValue() ) )
                        || ( !rental.getType().equals( ProcessTypes.FINANCIAL.getValue() ) ) ) {
                    if( rental.getCustomer().getLastname() == null || rental.getCustomer().getFirstname() == null
                            || rental.getCustomer().getFirstname().trim().isEmpty()
                            || rental.getCustomer().getLastname().trim().isEmpty() ) {
                        if( rental.getCustomer().getCompanyName() != null && !rental.getCustomer().getCompanyName().trim().isEmpty() ) {
                            if( !rental.getProcessFlags().isDevaiteDriver() ) {
                                throw new CarGuideProcessException( "Kein Fahrer eingetragen.", 9, 52, true );
                            }
                        }
                        else {
                            throw new CarGuideProcessException( "Name des Kunden nicht vollständig.", 9, 52, true );
                        }
                    }
                    if( rental.getProcessFlags().isDevaiteDriver() ) {
                        if( rental.getDriver().getLastname() == null || rental.getDriver().getLastname().trim().isEmpty()
                                || rental.getDriver().getFirstname() == null || rental.getDriver().getFirstname().trim().isEmpty() ) {
                            throw new CarGuideProcessException( "Name des Fahrers nicht vollständig.", 9, 52, true );
                        }
                    }
                    if( !rental.getProcessFlags().isDriverLicenseInspection()
                            && !rental.getType().equals( ProcessTypes.TRANSPORTATION.getValue() ) ) {
                        throw new CarGuideProcessException( "Sichtung des Führerscheins bitte bestätigen.", 9, 52, true );
                    }
                    else {
                        ProcessCustomerCard driverCard = rental.getDriver().getDriverCard();
                        if( driverCard.getNumber() == null || driverCard.getNumber().isEmpty()
                                || driverCard.getProcessDriverCategories().size() < 1 ) {
                            throw new CarGuideProcessException( "Keine Führerschein Nr. oder Führerscheinklasse eingetragen.", 9, 52,
                                    true );
                        }
                    }
                }
                if( rental.getMisc().getStartKm().intValue() < 0 ) {
                    throw new CarGuideProcessException( "Abfahrt KM zu klein.", 9, 64, true );
                }

                if( rental.getUsers().getMileageConfirmationUser() == null ) {
                    rental.getUsers().setMileageConfirmationUser( hireUserDao.getUserById( getHireUser().getUserId() ) );
                    //TODO C: Maybee needed later
                    //throw new CarGuideProcessException( "Bitte Abfahrtskilometerstand bestätigen.", 9, 64, true );
                }                                               

                List < ViewPlannerPosition > processes = searchProcessesForVehicle( rental.getProcessVehicle().getVehicle(), null, null, false );
                for( ViewPlannerPosition process : processes ) {
                    if( rental.getProcessNumber().equals( process.getProcessNumber() ) ) {
                        continue;
                    }
                    if( finance && process.getProcessType() == 4 && process.getProcessStatus() == 2 ) {
                        Loggable.logS( "Andere aktive Finanzierung: ID:" + process.getProcessId() + " Vorgang: " + process.getProcessNumber() );
                        throw new CarGuideProcessException( "Fahrzeug hat bereits eine aktive Finanzierung.", 8, 12, true );
                    }
                    else if( !finance && process.getProcessType() < 4 && process.getProcessStatus() == 2 ) {
                        Loggable.logS(
                                "Anderen aktiven Vorgang gefunden: ID:" + process.getProcessId() + " Vorgang: " + process.getProcessNumber() );
                        throw new CarGuideProcessException( "Fahrzeug hat bereits einen aktiven Vorgang. Bitte " + process.getProcessNumber() +" zuerst zurücknehmen oder Reservierung löschen.", 8, 12,
                                true );
                    }
                }

                //After process checks, create and store settings for contract
                rental.getUsers().setAgreement1User( hireUserDao.getUserById( getHireUser().getUserId() ) );
                log( "Ändere Status von 1 auf 2 für Vorgang " + rental.getId() );
                rental.setStatus( Integer.valueOf( 2 ) );
                
                //creates process periods
                //check if process has exisiting period, when its an reseted process 
                if( rental.getActiveProcessPeriods().size() < 1 ) {
                    log( "No periods found in process" );
                    Set < TransferProcessPeriod > transferPeriods = ProcessPeriod
                            .convertToTransferProcessPeriod( rental.getProcessPeriods() );
                    ProcessPeriodCalculator periodClaculator = new ProcessPeriodCalculator(
                            ProcessStatus.getStatusFromInt( rental.getStatus() ), rental.getRentalDate().getStartTime(),
                            rental.getRentalDate().getCurrentReturnTime(), rental.getRate().getUnit(),
                            rental.getRate().getPeriodStartDate(), transferPeriods );
                    transferPeriods = periodClaculator.fallbackForPeriodes();
                    rental.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, rental ) );
                }

                if( !rental.getType().equals( Integer.valueOf( 3 ) ) ) {
                    try {
                        IHireProcessCustomerService processCustomerService = CompactFactory
                                .createService( IHireProcessCustomerService.class );
                        //Überprüfe auf Äquivalenz für Rechnungsempfänger, wenn vorhanden
                        if( rental.getProcessFlags().isDevaiteInvoiceRecipient() ) {
                            Customer c = processCustomerService.searchCustomer( ( ProcessCustomer ) rental.getInvoiceRecipient().clone(),
                                    rental.getCompany() );
                            if( c != null ) {
                                rental.getInvoiceRecipient().setCustomer( c );
                                rental.getInvoiceRecipient().setCustomerNo( c.getCustomerno() );
                            }
                        }
                        //Überprüfe auf Äquivalenz für Kunden
                        Customer c = processCustomerService.searchCustomer( ( ProcessCustomer ) rental.getCustomer().clone(),
                                rental.getCompany() );
                        if( c != null ) {
                            rental.getCustomer().setCustomer( c );
                            rental.getCustomer().setCustomerNo( c.getCustomerno() );
                        }
                    }
                    catch( Exception e ) {
                        rental.setStatus( 1 );
                        log( "ERROR beim Abgleich des ProcessCustomer mit Id: "
                                + ( rental.getProcessFlags().isDevaiteInvoiceRecipient() ? rental.getInvoiceRecipient()
                                        : rental.getCustomer() ).getId() );
                        e.printStackTrace();
                    }
                    //this.searchCustomer( rental );
                    if( !rental.getProcessFlags().isDevaiteDriver() && rental.getCustomer().getCustomer() != null ) {
                        //Synchronisiere Customer mit ProcessCustomer
                        log( "update Customer" );
                        try {
                            IHireCustomerService customerService = CompactFactory.createService( IHireCustomerService.class );
                            customerService.updateCustomerOverRental( rental.getId() );
                        }
                        catch( Exception e ) {
                            e.printStackTrace();
                        }
                        log( "has updatet" );
                    }
                }
                if( rental.getCheckList().getDamageOut() != null && rental.getCheckList().getDamageOut().length() > 0 ) {
                    IHireVehicleService vehicleService = CompactFactory.createService( IHireVehicleService.class );
                    Vehicle vehicle = vehicleService.getVehicleById( rental.getProcessVehicle().getVehicle().getId() );
                    vehicle.getDamage().setDescription( rental.getCheckList().getDamageOut() );
                    rental.getCheckList().setDamageIn( rental.getCheckList().getDamageOut() );
                    vehicleService.updateVehicle( vehicle );
                }
            }
            else if( rental.getStatus().intValue() == ProcessStatus.BACK.getValue() && rental.getUsers().getAgreement2User() == null ) {
                rental.getUsers().setAgreement2User( hireUserDao.getUserById( getHireUser().getUserId() ) );
            }

            InspectRental.checkProcessCustomers( rental );

            this.updateRental( rental );

            try {
                RentalPrice price = calculateNewRentalPrices( rental, rental.getFirstPeriod(), false );
                if( rental.getType().equals( Integer.valueOf( 3 ) ) ) {
                    writeJSONDocumentFromRental( rental, DocumentType.INTERNAL_DRIVE, rental.getProcessNumber(), price,
                            rental.getFirstPeriod(), new Date() );
                }
                else {
                    writeJSONDocumentFromRental( rental, DocumentType.CONTRACT, rental.getProcessNumber(), price, rental.getFirstPeriod(),
                            new Date() );
                }
                return true;
            }
            catch( Exception e ) {
                e.printStackTrace();
                throw new CarGuideException( "ERROR beim Erzeugen des Vertrages.", 999, 0 );
            }
        }
        catch( CarGuideProcessException c ) {
            throw c;
        }
        catch( Exception e ) {
            log( "ERROR while printing" );
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean printFinalInvoice( Integer rentalId ) throws CarGuideException
    {
        try {
            log();
            this.getCurrentSession();
            if( rentalId == null || rentalId.intValue() <= 0 ) {
                throw new CarGuideException( "Id ist null", 901, 0 );
            }
            Rental process = rentalDao.getRentalById( rentalId );
            if( process == null ) {
                throw new CarGuideException( "Keinen Vorgang mit Id{" + rentalId + "} gefunden", 9, 21 );
            }
            if( !process.getStatus().equals( ProcessStatus.BACK.getValue() ) ) {
                log( "Rental is not back. In current status{" + process.getStatus() + "} it is not possible to create a final invoice." );
                return false;
            }

            InspectRental.checkProcessCustomers( process );

            RentalDate processDate = process.getRentalDate();
            // Secure check if returnTime after last period
            if( HIREPeriod.isDateAfterPeriod( processDate.getReturnTime(), process.getLastPeriod() ) ) {
                // Not normal, before create an final invoice, date must be saved with
                // updateRental().
                log( "WARNING: Update Rental object before calling this method." );
                return false;
            }

            //search all calculated periods after and between returnTime and set this as a credit and store then for later doing
            List < ProcessPeriod > periods = new ArrayList < ProcessPeriod >();
            periods.addAll( process.getActiveProcessPeriods() );
            Collections.sort( periods, new ProcessPeriodComparator() );
            Collections.reverse( periods );
            List < ProcessPeriod > calculatedPeriodsAfterReturn = new ArrayList < ProcessPeriod >();
            for( ProcessPeriod selectedPeriod : periods ) {
                if( selectedPeriod.hasInvoice() ) {
                    selectedPeriod.setStatus( ProcessPeriodStatus.VOUCHER.getValue() );
                    calculatedPeriodsAfterReturn.add( selectedPeriod );
                }
                if( HIREPeriod.isDateInPeriod( processDate.getReturnTime(), selectedPeriod ) ) {
                    break;
                }
            }

            //update periods with the new returnTime, 
            //delete all not calculatet periods after returnTime and 
            //create an new for period with returnTime
            Set < TransferProcessPeriod > transferPeriods = ProcessPeriod.convertToTransferProcessPeriod( process.getProcessPeriods() );
            ProcessPeriodCalculator periodClaculator = new ProcessPeriodCalculator( ProcessStatus.getStatusFromInt( process.getStatus() ),
                    process.getRentalDate().getStartTime(), process.getRentalDate().getCurrentReturnTime(), process.getRate().getUnit(),
                    process.getRate().getPeriodStartDate(), transferPeriods );
            transferPeriods = periodClaculator.refreshCalculatedPeriods();
            process.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, process ) );

            //get the new created period at the end:
            ProcessPeriod lastPeriod = process.getLastPeriod();
            RentalPrice price = calculateNewRentalPrices( process, lastPeriod, false );
            BigDecimal grossCreditAmount = new BigDecimal( 0 );
            BigDecimal netCreditAmount = new BigDecimal( 0 );
            BigDecimal taxCreditAmount = new BigDecimal( 0 );
            BigDecimal subTotalAmount = new BigDecimal( 0 );

            log( "after:" + calculatedPeriodsAfterReturn.size() );

            if( calculatedPeriodsAfterReturn.size() > 0 ) {
                List < ProcessPricePositions > creditPositions = new ArrayList < ProcessPricePositions >();
                for( ProcessPeriod calculatedPeriod : calculatedPeriodsAfterReturn ) {
                    RentalPrice periodCreditPrice = this.createCreditPositionsFromInvoiceDocument( calculatedPeriod.getInvoice(), true );
                    price.getProcessPricePositions().addAll( periodCreditPrice.getProcessPricePositions() );
                    log( calculatedPeriod.getInvoice().getDocumentNumber() );
                    for( ProcessPricePositions position : periodCreditPrice.getProcessPricePositions() ) {
                        position.setComment( "Gutschrift für Rechnung " + calculatedPeriod.getInvoice().getDocumentNumber() );
                        grossCreditAmount = grossCreditAmount.add( position.getCompletePriceGross() );
                        netCreditAmount = netCreditAmount.add( position.getCompletePrice() );
                        taxCreditAmount = taxCreditAmount.add( position.getTaxPrice() );
                        subTotalAmount = subTotalAmount.add( position.getCompletePrice() );
                        position.setPrice( price );
                    }
                }
            }
            //calculate the new end price and create invoice document
            price.setCalculateInvoiceAmountGross( price.getCalculateInvoiceAmountGross().add( grossCreditAmount ) );
            price.setCalculateInvoiceAmountNet( price.getCalculateInvoiceAmountNet().add( netCreditAmount ) );
            price.setTaxPrice( price.getTaxPrice().add( taxCreditAmount ) );
            price.setSubTotal( price.getSubTotal().add( subTotalAmount ) );
            price.setManualInvoiceAmount( price.getCalculateInvoiceAmountGross().subtract( price.getPrepayment() ) );
            price.setFinalInvoice( true );

            String invoiceNo = this.buildInvoiceNo( process.getBranch() );
            process.setStatus( ProcessStatus.CLOSED.getValue() );
            createProcessDocuments( process, DocumentType.INVOICE, invoiceNo, price, lastPeriod, new Date() );
            rentalDao.updateRental( process );
            return writeJSONDocumentFromRental( process, DocumentType.INVOICE, invoiceNo, price, lastPeriod, new Date() );

        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public String printInvoice( final Integer rentalId, final Integer periodId, final String invoiceNo, final Date selectedInvoiceDate ) throws CarGuideException
    {
        try {
            this.getCurrentSession();
            Rental rental = rentalDao.getRentalById( rentalId );
            if( rental.getType().equals( ProcessTypes.FINANCIAL.getValue() ) ) {
                try {
                    InspectRental.checkProcessCustomers( rental );
                }
                catch( Exception e ) {
                    rental.setInvoiceRecipient(
                            ProcessCustomer.overwriteProcessCustomerToRecipient( rental.getCustomer(), rental.getInvoiceRecipient() ) );
                }
            }
            if( !InspectRental.inspectProcessForInvoice( rental, getHireUser() ) ) {
                return null;
            }
            InspectRental.checkProcessCustomers( rental );

            ProcessPeriod period = periodDao.getPeriodById( periodId );
            log( period );
            if( period == null ) {

                log( "No periods found in process" );
                Set < TransferProcessPeriod > transferPeriods = ProcessPeriod.convertToTransferProcessPeriod( rental.getProcessPeriods() );
                ProcessPeriodCalculator periodClaculator = new ProcessPeriodCalculator(
                        ProcessStatus.getStatusFromInt( rental.getStatus() ), rental.getRentalDate().getStartTime(),
                        rental.getRentalDate().getCurrentReturnTime(), rental.getRate().getUnit(), rental.getRate().getPeriodStartDate(),
                        transferPeriods );
                transferPeriods = periodClaculator.fallbackForPeriodes();
                rental.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, rental ) );

                period = rental.getFirstPeriod();
                //throw new CarGuideException( "Keinen Vermietungszeitraum gefunden", 9, 21 );
            }
            if( !period.getRental().getId().equals( rental.getId() ) ) {
                throw new CarGuideException( "Period from given process", 9, 21 );
            }

            if( getHireUser().getLoginname().equals( "SystemImport" ) ) {
                // TODO C: Feedback to calling system
                log( "System Benutzer können keine Original Rechnung ausdrucken" );
                return null;
            }
            Date documentDate = selectedInvoiceDate;
            try {
                RentalDate rentalDate = rental.getRentalDate();
                RentalPrice price = null;
                String workingInvoiceNo = null;
                // Create secure invoice number and settings
                if( invoiceNo == null && checkIfInvoiceAllowed( period ) ) {
                    price = this.calculateNewRentalPrices( rental, period, false );

                    rentalDate.setPrintInvoiceTime( new Date() );
                    rentalDate.setProcessEndTime( new Date() );
                    rental.getUsers().setInvoiceUser( hireUserDao.getUserById( getHireUser().getUserId() ) );

                    if( HIREPeriod.isLastOpenInvoice( rental, period ) ) {
                        if( rental.getStatus().equals( Integer.valueOf( ProcessStatus.BACK.getValue() ) ) ) {
                            rental.setStatus( ProcessStatus.CLOSED.getValue() );
                        }
                    }
                    workingInvoiceNo = this.buildInvoiceNo( rental.getBranch() );
                    createProcessDocuments( rental, DocumentType.INVOICE, workingInvoiceNo, price, period, documentDate );
                    this.updateRental( rental );
                }
                else {
                    ProcessDocument document = documentDao.getDocumentByNo( invoiceNo );
                    if( document != null && document.getDocumentType().equals( "INVOICE" ) ) {
                        workingInvoiceNo = document.getDocumentNumber();
                        documentDate = document.getDocumentDate();
                        price = document.getPrice();
                        log( "use existing invoice{" + workingInvoiceNo + "}" );
                    }
                    else {
                        log( "WARNING: Document not existing as INVOICE" );
                        return null;
                    }
                }
                try {
                    return writeJSONDocumentFromRental( rental, DocumentType.INVOICE, workingInvoiceNo, price, period, documentDate )
                            ? workingInvoiceNo
                            : null;
                }
                catch( CarGuideException c ) {
                    c.printStackTrace();
                    throw new CarGuideProcessException(
                            "Fehler bei Erstellung der Rechnungs-Nr. Angaben fehlen in den Firmeneinstellungen.", 2, 26, true );
                }
            }
            catch( Exception e ) {
                e.printStackTrace();
                throw new CarGuideException( "Fehler beim Erzeugen/Speichern der Rechnung", 999, 0 );
            }

        }
        catch( CarGuideProcessException c ) {
            throw c;
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String printCreditVoucher( Integer rentalId, String invoiceNumber ) throws CarGuideProcessException
    {

        try {
            this.getCurrentSession();
            if( rentalId == null || rentalId.intValue() < 1 || invoiceNumber == null || invoiceNumber.length() < 1 ) {
                throw new CarGuideProcessException( "Keine gültigen Eingabewerte", 999, 0, true );
            }
            // create needed objects and sort periods from process
            Rental process = getRentalById( rentalId );
            if( !this.getHireUser().hasRight( HireRights.FUHRPARKLEITER, process ) ) {
                throw new CarGuideException( "Keine Berechtigung für das Auslösen der Gutschriften", 999, 0 );
            }
            Set < ProcessDocument > documents = process.getProcessDocuments();
            List < ProcessPeriod > periods = new ArrayList < ProcessPeriod >();
            periods.addAll( process.getActiveProcessPeriods() );
            Collections.sort( periods, new ProcessPeriodComparator() );

            // search ProcessDocument for given invoiceNo
            ProcessDocument invoiceDocument = null;
            for( ProcessDocument document : documents ) {
                if( document.getDocumentNumber().equals( invoiceNumber ) ) {
                    if( document.getDocumentType().equals( DocumentType.CREDIT.toString() ) ) {
                        List < ProcessPeriod > documentPeriods = new ArrayList < ProcessPeriod >();
                        documentPeriods.addAll( document.getPeriods() );
                        if( documentPeriods.size() > 1 ) {
                            throw new CarGuideProcessException( "Kann keine Gutschrift erzeugen.Zuviele Zeitperoden.", 999, 0, true );
                        }
                        return writeJSONDocumentFromRental( process, DocumentType.CREDIT, invoiceNumber, document.getPrice(),
                                documentPeriods.get( 0 ), document.getDocumentDate() ) ? invoiceNumber : null;
                    }
                    else if( document.getDocumentType().equals( DocumentType.INVOICE.toString() ) ) {
                        // save founded invoice
                        invoiceDocument = document;
                    }
                }
            }

            if( invoiceDocument == null ) {
                log( "No invoice document found" );
                throw new CarGuideProcessException( "Keine Rechnung gefunden.", 999, 0, true );
            }
            // Check if a higher period is been calculated
            List < ProcessPeriod > documentPeriods = new ArrayList < ProcessPeriod >();
            documentPeriods.addAll( invoiceDocument.getPeriods() );
            ProcessPeriod documentPeriod = documentPeriods.get( 0 );
            if( !this.getHireUser().isCompact() ) {
                ProcessPeriod selectedPeriod = documentPeriods.get( 0 );
                for( int i = documentPeriods.get( 0 ).getSortingSequence() + 1; i < periods.size(); ++i ) {
                    selectedPeriod = periods.get( i );
                    if( selectedPeriod.isCalculated() ) {
                        log( "Credit not possible. Next invoice already calculated." );
                        throw new CarGuideProcessException( "Gutschrift nicht möglich. Folge-Rechnung schon berechnet.", 999, 0, true );
                    }
                }
            }

            // copy price object and positions for credit
            RentalPrice creditPrice = createCreditPositionsFromInvoiceDocument( invoiceDocument, false );

            // build document and json
            String creditNo = this.buildInvoiceNo( process.getBranch() );
            Date documentDate = new Date();
            createProcessDocuments( process, DocumentType.CREDIT, creditNo, creditPrice, documentPeriod, documentDate );
            writeJSONDocumentFromRental( process, DocumentType.CREDIT, creditNo, creditPrice, documentPeriod, documentDate );

            // now, remove ProcessPeriod and calculate them new
            Set < TransferProcessPeriod > transferPeriods = ProcessPeriod.convertToTransferProcessPeriod( process.getProcessPeriods() );
            ProcessPeriodCalculator periodClaculator = new ProcessPeriodCalculator( ProcessStatus.getStatusFromInt( process.getStatus() ),
                    process.getRentalDate().getStartTime(), process.getRentalDate().getCurrentReturnTime(), process.getRate().getUnit(),
                    process.getRate().getPeriodStartDate(), transferPeriods );
            transferPeriods = periodClaculator.refreshCalculatedPeriods();
            process.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, process ) );

            //set process status back and save changes in process
            if( process.getStatus().intValue() >= ProcessStatus.CLOSED.getValue() ) {
                process.setStatus( Integer.valueOf( ProcessStatus.BACK.getValue() ) );
            }
            rentalDao.updateRental( process );
            return creditNo;

        }
        catch( CarGuideProcessException c ) {
            throw c;
        }
        catch( Exception e ) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Create from invoiceDocument a credit
     * 
     * @param invoiceDocument
     * @return
     */
    private RentalPrice createCreditPositionsFromInvoiceDocument( final ProcessDocument invoiceDocument, boolean negativeValues )
    {

        Set < ProcessPricePositions > creditPositions = new HashSet < ProcessPricePositions >();
        RentalPrice invoicePrice = invoiceDocument.getPrice();
        // create now the RentalPrice for the new credit
        RentalPrice creditPrice = new RentalPrice( invoicePrice.getPrepayment(), invoicePrice.getLiabilityReductionPrice(),
                invoicePrice.getAccidentInsurancePrice(), invoicePrice.getCalculateInvoiceAmountGross(),
                invoicePrice.getCalculateInvoiceAmountNet(), invoicePrice.getManualInvoiceAmount(), invoicePrice.getFuelPrice(),
                invoicePrice.getCollectionPrice(), invoicePrice.getDeliveryPrice(), invoicePrice.getDeductible(), false, null, null,
                invoicePrice.getSubTotal(), invoicePrice.getServicePrice(), invoicePrice.getTax(), invoicePrice.getTaxPrice() );
        // create now the credit positions
        for( ProcessPricePositions position : invoicePrice.getProcessPricePositions() ) {
            ProcessPricePositions creditPosition = new ProcessPricePositions( creditPrice, position.getUnit(), position.getPositionName(),
                    position.getCount(),
                    negativeValues ? position.getPricePerUnit().multiply( new BigDecimal( -1 ) ) : position.getPricePerUnit(),
                    negativeValues ? position.getCompletePrice().multiply( new BigDecimal( -1 ) ) : position.getCompletePrice(),
                    position.getTaxPercent(),
                    negativeValues ? position.getTaxPrice().multiply( new BigDecimal( -1 ) ) : position.getTaxPrice(),
                    negativeValues ? position.getCompletePriceGross().multiply( new BigDecimal( -1 ) ) : position.getCompletePriceGross(),
                    "C" );

            creditPositions.add( creditPosition );
        }
        creditPrice.setProcessPricePositions( creditPositions );
        return creditPrice;
    }

    //TODO A: needs implementation check about documents from this period too
    private boolean checkIfInvoiceAllowed( final ProcessPeriod period )
    {
        if( period.hasInvoice() ) {
            log( "Not allowed to generate new invoice no" );
            return false;
        }
        return true;
    }

    @Override
    public boolean printSubrogationAgreement( Integer rentalId ) throws CarGuideException
    {
        this.getCurrentSession();
        if( rentalId == null || rentalId.intValue() <= 0 ) {
            throw new CarGuideException( "Id ist null", 901, 0 );
        }
        Rental rental = rentalDao.getRentalById( rentalId );
        if( rental == null ) {
            throw new CarGuideException( "Kein Vermietung gefunden", 9, 21 );
        }
        if( !getHireUser().getUserId().equals( rental.getUser().getId() )
                && !getHireUser().hasRight( HireRights.FUHRPARKLEITER, rental.getBranch() )
                && !getHireUser().hasSameRights( rental.getUser(), HireRights.VERMITTLER, rental ) ) {
            throw new CarGuideException( "Nicht genügend Rechte", 20, 12 );
        }
        if( !rental.getProcessFlags().isAccident() ) {
            throw new CarGuideException( "Vorgang ist nicht berechtigt eine Abtretungserklärung zu drucken.", 9, 59 );
        }
        writeJSONSubrogationAgreement( rental );
        //TODO A: remove unused code and implements DataService for JSON
        //        byte[] content = null;
        //        IHireDataService dataService = CompactFactory.createService( IHireDataService.class );
        //        List < Data > datas = dataService.getDataByType( ( Rental ) rental.clone(), "PROCESS_SUBROGATION_AGREEMENT" );
        //        if( datas.size() == 0 ) {
        try {
            //                ServiceProcess s = createServiceProcess( rental );
            //                IPDFSERVICE pdfService = CompactFactory.createCompActService( IPDFSERVICE.class );//AWX
            //                HashMap < String, Object > options = new HashMap < String, Object >();
            //                options.put( "rental", s );
            //                options.put( "pdfname", "Accident" );
            //                content = pdfService.getPDF( 4, 7, options );//AWX
            //                if( content == null ) {
            //                    throw new NullPointerException( "PDF ist null" );
            //                }
            //                dataService.addData( new Data( "PROCESS_SUBROGATION_AGREEMENT", "PROCESS", rental.getId(), content ),
            //                        ( Rental ) rental.clone() );
            rental.getUsers().setSubrogationAgreementUser( hireUserDao.getUserById( getHireUser().getUserId() ) );
            rentalDao.updateRental( rental );
        }
        catch( Exception e ) {
            e.printStackTrace();
            throw new CarGuideException( "Fehler beim Erzeugen der Abtretungserklärung", 999, 0 );
        }
        //        }
        //        else {
        //            if( !datas.get( 0 ).getDataType().equals( "PROCESS_SUBROGATION_AGREEMENT" ) ) {
        //                throw new CarGuideException( "Fehler beim Abholen der Abtretungserklärung", 999, 0 );
        //            }
        //            content = datas.get( 0 ).getData();
        //        }
        //        HirePDF pdf = new HirePDF();
        //        pdf.setFileName( "/tmp/" + "abtretung.pdf" );
        //        pdf.setPdfData( content );
        //        return pdf;
        return true;
    }

    /**
     * Zentrale Methode zur Neu Berechnung der Preise und Kosten
     */
    @Override
    public RentalPrice calculateNewRentalPrices( Rental rental, ProcessPeriod period, boolean save ) throws CarGuideException
    {
        this.getCurrentSession();
        try {
            if( !InspectRental.inspectProcessCalculation( rental, getHireUser() ) ) {
                return null;
            }
            log( "called with params {" + rental.getId() + "}, Period{" + period + "} {" + save + "}" );

            Set < TransferProcessPeriod > transferPeriods = ProcessPeriod.convertToTransferProcessPeriod( rental.getProcessPeriods() );
            ProcessPeriodCalculator periodClaculator = new ProcessPeriodCalculator( ProcessStatus.getStatusFromInt( rental.getStatus() ),
                    rental.getRentalDate().getStartTime(), rental.getRentalDate().getCurrentReturnTime(), rental.getRate().getUnit(),
                    rental.getRate().getPeriodStartDate(), transferPeriods );
            if( period == null ) {
                log( "No periods found in process" );

                transferPeriods = periodClaculator.fallbackForPeriodes();
                rental.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, rental ) );

                period = rental.getFirstPeriod();
                //throw new CarGuideException( "Period is null", 901, 0 );
            }
            else {
                //transferPeriods = periodClaculator.refreshCalculatedPeriods();
                //rental.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, rental ) );
            }

            RentalMiscellaneous misc = rental.getMisc();
            //Nur Für Probefahrten und Vermietungen
            //Ermittle Rechnungspositionen
            if( !rental.getType().equals( ProcessTypes.TRANSPORTATION.getValue() ) ) {
                RentalPrice price = new RentalPrice();
                Map < ProcessPricePositionTypes, RentalPricePosition > pricePositions = new HashMap < ProcessPricePositionTypes, RentalPricePosition >();
                Set < ProcessPricePositions > positions = price.getProcessPricePositions();

                log( "Berechnung der Rechnungspositionen" );
                price.setSubTotal( new BigDecimal( 0 ) );
                ProcessActiveRate rate = rental.getRate();

                //String special = ""; 
                final Map < String, List < ProcessTime > > timeMap = new ProcessTimeCalculator().calculateProcessPeriod( period,
                        rate.getUnit() );
                //final List < ProcessTime > completePeriod = timeMap.get( ProcessTime.COMPLETE );
                final List < ProcessTime > overrunPeriod = timeMap.get( ProcessTime.OVERRUN );
                final List < ProcessTime > drivingPeriod = timeMap.get( ProcessTime.DRIVING );
                //Berechnung des Tarifes nur bei Vermietung notwendig
                if( !rate.isHasFixPrice() ) {
                    //Bei Tarifpreis
                    BigDecimal divisor = null;
                    if( rate.getUnit().equals( "DAY" ) ) {
                        divisor = new BigDecimal( "1" );
                    }
                    else if( rate.getUnit().equals( "WEEK" ) ) {
                        divisor = new BigDecimal( "7" );
                    }
                    else if( rate.getUnit().equals( "MONTH" ) ) {
                        divisor = new BigDecimal( "30" );
                    }
                    //Tarif
                    if( drivingPeriod != null && drivingPeriod.size() > 0 ) {
                        for( ProcessTime processTime : drivingPeriod ) {
                            if( processTime != null && processTime.getCount().compareTo( new BigDecimal( 0 ) ) > 0 ) {
                                if( processTime.getUnit().equals( ProcessTime.DAY ) ) {
                                    if( rate.getUnit().equals( "DAY" ) ) {
                                        pricePositions.put( ProcessPricePositionTypes.CHARGEDAY, new RentalPricePosition(
                                                processTime.getCount(), rate.getUnitPrice(), "Tage", price.getTax() ) );
                                    }
                                    else {
                                        pricePositions.put( ProcessPricePositionTypes.CHARGEOTHERDAY,
                                                new RentalPricePosition( processTime.getCount(),
                                                        rate.getUnitPrice().divide( divisor, 2, RoundingMode.HALF_UP ), "Tage",
                                                        price.getTax() ) );
                                    }
                                }
                                else if( processTime.getUnit().equals( ProcessTime.WEEK ) ) {
                                    pricePositions.put( ProcessPricePositionTypes.CHARGEWEEK, new RentalPricePosition(
                                            processTime.getCount(), rate.getUnitPrice(), "Wochen", price.getTax() ) );
                                }
                                else if( processTime.getUnit().equals( ProcessTime.MONTH ) ) {
                                    pricePositions.put( ProcessPricePositionTypes.CHARGEMONTH, new RentalPricePosition(
                                            processTime.getCount(), rate.getUnitPrice(), "Monate", price.getTax() ) );
                                }
                                else {
                                    throw new Exception( "Unbekannte Zeiteinheit." );
                                }
                            }
                        }
                    }
                    //Überziehung
                    if( overrunPeriod != null && overrunPeriod.size() == 1 ) {
                        ProcessTime processTime = overrunPeriod.get( 0 );
                        if( processTime != null && processTime.getCount().compareTo( new BigDecimal( 0 ) ) > 0 ) {
                            pricePositions.put( ProcessPricePositionTypes.OVERRUN, new RentalPricePosition( processTime.getCount(),
                                    rate.getUnitPrice().divide( divisor, 2, RoundingMode.HALF_UP ), "Überzogene Tage", price.getTax() ) );
                        }
                    }
                }
                else {
                    //Festpreis
                    if( drivingPeriod != null && drivingPeriod.size() == 1 ) {
                        BigDecimal divisor = new ProcessTimeCalculator().calculateProcessPeriod( period, "DAY" ).get( ProcessTime.DRIVING )
                                .get( 0 ).getCount();
                        ProcessTime processTime = drivingPeriod.get( 0 );
                        if( processTime != null && processTime.getCount().compareTo( new BigDecimal( 0 ) ) > 0 ) {
                            pricePositions.put( ProcessPricePositionTypes.FIXEDCHARGE, new RentalPricePosition( new BigDecimal( 1 ),
                                    "Festpreis", rate.getFixPrice(), rate.getFixPrice(), price.getTax() ) );
                        }
                        //Überziehung
                        if( overrunPeriod != null && overrunPeriod.size() == 1 ) {
                            processTime = overrunPeriod.get( 0 );
                            if( processTime != null && processTime.getCount().compareTo( new BigDecimal( 0 ) ) > 0 ) {
                                pricePositions.put( ProcessPricePositionTypes.OVERRUN,
                                        new RentalPricePosition( processTime.getCount(),
                                                rate.getFixPrice().divide( divisor, 2, RoundingMode.HALF_UP ), "Überzogene Tage",
                                                price.getTax() ) );
                            }
                        }
                    }
                }
                if( rental.getStatus().intValue() > ProcessStatus.RESERVED.getValue()
                        && rental.getLastPeriod().getSortingSequence() == period.getSortingSequence() ) {
                    //Calculate the KM. Only in the last invoice.
                    if( misc.getEndKm() != null && misc.getEndKm().compareTo( new BigDecimal( 0 ) ) > 0 && misc.getStartKm() != null
                            && misc.getStartKm().compareTo( new BigDecimal( 0 ) ) > -1 ) {
                        HIREInvoicingKM invoicingKM = new HIREInvoicingKM( rental );
                        Map < KMStatus, BigDecimal > invoicingKMMap = invoicingKM.calculatePosition();
                        RentalPricePosition positionKM = invoicingKM.getPosition();
                        misc.setAllowedKM( invoicingKMMap.get( KMStatus.ALLOWED ) );
                        misc.setDrivenkm( invoicingKMMap.get( KMStatus.DRIVEN ) );
                        misc.setKmTooMuch( new BigDecimal( 0 ) );
                        if( positionKM != null ) {
                            pricePositions.put( ProcessPricePositionTypes.DRIVENKM, positionKM );
                            misc.setKmTooMuch( invoicingKMMap.get( KMStatus.OVERRUN ) );
                        }
                    }
                }
                //            //Berechnung der Treibstoffkosten
                //            if( misc.getFuelLiter() != null && misc.getFuelLiter().doubleValue() > 0 && price.getFuelPrice() != null
                //                    && price.getFuelPrice().doubleValue() > 0 ) {
                //                RentalPricePosition position = new RentalPricePosition( misc.getFuelLiter(), price.getFuelPrice(), "l", price.getTax() );
                //                pricePositions.put( "fuel", position );
                //                //price.setSubTotal( price.getSubTotal().add( position.getCompletePrice() ) );
                //            }

                // Umwandlung der RentalPricePosition in ProcessPricePositions und Berechnung der 
                if( pricePositions.size() > 0 ) {
                    for( ProcessPricePositionTypes key : pricePositions.keySet() ) {
                        price.setSubTotal( price.getSubTotal().add( pricePositions.get( key ).getCompletePrice() ) );
                        ProcessPricePositions position = pricePositions.get( key ).toProcessPricePosition();
                        position.setPrice( price );
                        position.setPositionName( key.getValue() );
                        position.setType( "I" );
                        price.getProcessPricePositions().add( position );
                        //Setzen der einzelnen Preispositionen
                        price.setPricePositions( pricePositions );
                    }
                }

                //Lieferpreis
                //            if( departure.isDelivery() ) {
                //                price.setServicePrice( price.getServicePrice().add( rate.getDeliverPrice() ) );
                //                price.setDeliveryPrice( rate.getDeliverPrice() );
                //                pricePositions.put( ProcessPricePositionTypes.DELIVERY,
                //                        new RentalPricePosition( null, "Lieferung", null, rate.getDeliverPrice(), price.getTax() ) );
                //            }
                //Abholpreis
                //            if( departure.isCollection() ) {
                //                price.setServicePrice( price.getServicePrice().add( rate.getCollectionPrice() ) );
                //                price.setCollectionPrice( rate.getCollectionPrice() );
                //                pricePositions.put( ProcessPricePositionTypes.COLLECTION,
                //                        new RentalPricePosition( null, "Abholung", null, rate.getCollectionPrice(), price.getTax() ) );
                //            }
                //Berechnen der Zwischensumme
                price.setCalculateInvoiceAmountNet( price.getServicePrice().add( price.getSubTotal() ) );
                //Berechnung der Umsatzsteuer
                Tax usedTax = taxDao.getTaxByRegionAndPeriod( "GERMANY", period.getEnd() );
                if( usedTax == null ) {
                    log( "Error: no tax found for region: " + "GERMANY" + " period end: " + period.getEnd() );
                }
                else {
                    VOLATaxCalculation taxCalc = new VOLATaxCalculation( usedTax, price.getCalculateInvoiceAmountNet() );
                    price.setTax( taxCalc.getTaxInPercent() );
                    price.setTaxPrice( taxCalc.getVatAmount() );
                    price.setCalculateInvoiceAmountGross( taxCalc.getAmountGross() );

                    //Berechnung des zu zahlenden Betrages
                    price.setManualInvoiceAmount( price.getCalculateInvoiceAmountGross().subtract( price.getPrepayment() ) );
                    //rental.getMisc().setSpecialArrangement( special ); 
                    return price;
                }

            }
            else {
                log( "Berechne KM für Interne Fahrten" );
                //Berechnung der KM für Interne Fahrten
                if( misc.getDrivenkm() == null && misc.getEndKm() != null && misc.getEndKm().compareTo( new BigDecimal( 0 ) ) > 0
                        && misc.getStartKm() != null && misc.getStartKm().compareTo( new BigDecimal( 0 ) ) > 0 ) {
                    misc.setDrivenkm( misc.getEndKm().subtract( misc.getStartKm() ) );
                    misc.setKmTooMuch( new BigDecimal( 0 ) );
                }
                return new RentalPrice();
            }
        }
        catch( Exception e ) {
            log( "Error in calculation: " );
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Build a invoice no for given branch
     * 
     * @param branch
     * @return
     * @throws CarGuideException
     */
    @Transactional
    private String buildInvoiceNo( CompanyBranch branch ) throws CarGuideException
    {
        IHireCompanyService companyService = CompactFactory.createService( IHireCompanyService.class );
        Integer branchId = branch.getId();
        if( accountingDao.hasAccountingForDebitors( branchId ) ) {
            String invoiceCount = accountingDao.provideInvoiceNumberForBranchId( branchId );
            if( invoiceCount == null || invoiceCount.isEmpty() ) {
                log( "no counting part for invoice" );
                throw new CarGuideException( "Keinen Rechnungscounter", 2, 26 );
            }
            SimpleDateFormat yearFormater = new SimpleDateFormat( "yy" );
            Date currentDate = new Date();
            String year = yearFormater.format( currentDate );
            DecimalFormat formatNo = new DecimalFormat( "000000" );

            if( branch.getDetail().getCode() == null || branch.getDetail().getCode().equals( "" ) ) {
                log( "Branch has no Branch Code" );
                throw new CarGuideException( "Branch has no Branch Code.", 2, 26 );
            }
            String invoiceNo = branch.getDetail().getCode() + year + formatNo.format( Integer.valueOf( invoiceCount ) );
            log( "invoiceNo: " + invoiceNo );
            if( invoiceNo != null && !invoiceNo.isEmpty() ) return invoiceNo;
        }
        //old process to generate invoice number
        else if( branch.getBranchInvoice() != null ) {
            BranchInvoice invoice = branch.getBranchInvoice();
            if( invoice.getCurrentInvoiceNo().intValue() == 0
                    || invoice.getCurrentInvoiceNo().compareTo( invoice.getEndInvoiceNo() ) == 1 ) {
                invoice.setCurrentInvoiceNo( invoice.getStartInvoiceNo() );
            }
            String invoiceNo = "" + invoice.getCurrentInvoiceNo();
            invoice.setCurrentInvoiceNo( Integer.valueOf( invoice.getCurrentInvoiceNo().intValue() + 1 ) );
            invoice.setLastInvoiceTime( new Date() );
            companyService.updateBranch( branch );
            log( "invoiceNo: " + invoiceNo );
            return invoiceNo;
        }
        throw new CarGuideException( "Company Rechnungsinformationen fehlen.", 2, 26 );
    }

    @Override
    public boolean deleteProcess( Integer processId, boolean deleted ) throws CarGuideException
    {
        try {
            this.getCurrentSession();
            if( processId == null || processId.intValue() < 1 ) {
                throw new CarGuideException( "processId ist null oder kein gültiger Vorgang.", 901, 0 );
            }
            Rental process = this.getRentalById( processId );
            if( deleted ) {
                if( this.getHireUser().hasRight( HireRights.VERMITTLER, process )
                        && process.getStatus().compareTo( ProcessStatus.DRIVING.getValue() ) < 0 ) {
                    return rentalDao.deleteProcess( process );
                }

                if( process.getStatus().compareTo( ProcessStatus.DRIVING.getValue() ) == 0 ) {
                    if( this.getHireUser().hasRight( HireRights.VERMITTLER, process ) || this.getHireUser().isCompact() ) {
                        boolean canMarkAsDeleted = false;
                        if( process.getProcessPeriods() != null ) {
                            for( ProcessPeriod period : process.getProcessPeriods() ) {
                                if( period.isCalculated() ) {
                                    if( period.hasInvoice() ) {
                                        throw new CarGuideException( "Vorgang berechnet ohne Gutschrift", 20, 11 );
                                    }
                                }
                                canMarkAsDeleted = true;
                            }
                            if( canMarkAsDeleted ) {
                                process.setStatus( ProcessStatus.DELETED.getValue() );
                                process.getProcessFlags().setDeleted( true );
                                return this.rentalDao.updateRental( process );
                            }
                        }
                    }
                }
            }
            else {
                if( !getHireUser().hasRight( HireRights.VERMITTLER, process ) ) {
                    throw new CarGuideException( "Fehlendes Recht Vermittler", 20, 11 );
                }
                process.getProcessFlags().setDeleted( true );
                process.setStatus( ProcessStatus.DELETED.getValue() );
                return rentalDao.updateRental( process );
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public List < Rental > getProcessesWithOutCopyForAccounting( Company company ) throws CarGuideException
    {
        this.getCurrentSession();
        if( company == null ) {
            throw new CarGuideException( "Company exisitiert nicht", 999, 0 );
        }
        if( !getHireUser().isCompact() ) {
            throw new CarGuideException( "Kein CompAct MA", 20, 19 );
        }
        List < Rental > rentals = rentalDao.getProcessesWithOutCopyForAccounting( company.getId() );
        List < Rental > processes = new ArrayList < Rental >();
        for( Rental rental : rentals ) {
            processes.add( rental );
        }
        return processes;
    }

    @Override
    public List < ViewProcessOverview > searchProcessesByParams( final HireProcessSearchParams args, final Integer maxResult,
            final Integer page ) throws CarGuideException
    {
        this.getCurrentSession();
        if( args == null ) {
            throw new CarGuideException( "Keine Suchparameter angegeben", 999, 0 );
        }
        Integer userId = null;
        if( !getHireUser().isCompact() ) {
            userId = getHireUser().getUserId();
        }
        List < ViewProcessOverview > rentals = viewProcessOverviewDao.getProcessOverview( userId, args, maxResult, page );

        return rentals;
    }

    @Override
    public List < Rental > searchRentalByParams( final HireProcessSearchParams args, final Integer maxResult, final Integer page )
            throws CarGuideException
    {
        this.getCurrentSession();
        if( args == null ) {
            throw new CarGuideException( "Keine Suchparameter angegeben", 999, 0 );
        }
        Integer userId = null;
        if( !getHireUser().isCompact() ) {
            userId = getHireUser().getUserId();
        }
        List < Rental > rentals = rentalDao.getProcessOverview( userId, args, maxResult, page );

        return rentals;
    }

    public Long countProcessesByParams( HireProcessSearchParams args ) throws CarGuideException
    {
        this.getCurrentSession();
        if( args == null ) {
            throw new CarGuideException( "Keine Suchparameter angegeben", 999, 0 );
        }
        Integer userId = null;
        if( !getHireUser().isCompact() ) {
            userId = getHireUser().getUserId();
        }
        return viewProcessOverviewDao.countProcessByParams( userId, args );
    }

    @Override
    public List < ViewPlannerPosition > searchProcessesForVehicles( final List < Vehicle > vehicles, Date startTime, Date endTime,
            boolean all ) throws CarGuideException
    {
        try {
            this.getCurrentSession();
            if( vehicles == null || vehicles.size() == 0 ) {
                throw new CarGuideException( "Argument vehicles null oder leer", 999, 0 );
            }
            List < Integer > ids = new ArrayList < Integer >();
            for( Vehicle v : vehicles ) {
                if( getHireUser().hasRight( HireRights.VERMITTLER, v ) ) {
                    ids.add( v.getId() );
                }
            }
            if( ids.size() > 0 ) {
                List < ViewPlannerPosition > processes = new ArrayList < ViewPlannerPosition >();
                for( ViewPlannerPosition rental : viewPlannerPositionDao.searchProcessesForVehicles( ids, startTime, endTime, all ) ) {
                    processes.add( ( ViewPlannerPosition ) rental ); // without clone()
                }
                return processes;
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        return null;
    }

    public List < ViewPlannerPosition > searchProcessesForVehicle( final Vehicle vehicle, Date startTime, Date endTime, boolean all )
            throws CarGuideException
    {
        this.getCurrentSession();
        if( vehicle == null ) {
            throw new CarGuideException( "Keine gültigen Argumente.", 999, 0 );
        }
        List < Vehicle > vehicles = new ArrayList < Vehicle >();
        vehicles.add( vehicle );
        List < ViewPlannerPosition > returnList = new ArrayList < ViewPlannerPosition >();
        List < ViewPlannerPosition > foundProcesses = this.searchProcessesForVehicles( vehicles, startTime, endTime, all );
        if( foundProcesses != null ) {
            for( ViewPlannerPosition rental : foundProcesses ) {
                returnList.add( ( ViewPlannerPosition ) rental ); // without clone()
            }
        }
        return returnList;
    }

    //TODO A: needs new implementation, now with periods instead process
    /**
     * Method to get all calculated or possible caluclated processes Use for
     * accounting api
     */
    @Override
    public List < Rental > searchcalculatedProcesses( final Integer companyId, final Date startTime, final Date endTime )
            throws CarGuideException
    {
        this.getCurrentSession();
        if( companyId == null || startTime == null || endTime == null ) {
            throw new CarGuideException( "Keine gültigen Argumente.", 999, 0 );
        }
        IHireCompanyService companyService = CompactFactory.createService( IHireCompanyService.class );
        if( !getHireUser().hasRight( HireRights.FILIALLEITER, companyService.getCompany( companyId ) ) ) {
            throw new CarGuideException( "Nicht genügend Rechte", 20, 12 );
        }
        List < Rental > processes = rentalDao.searchClosedCalculatedProccesses( companyId, startTime, endTime );
        for( Rental process : processes ) {
            process.getCustomer().getProcessCustomerCards().size();
            process.getInvoiceRecipient().getProcessCustomerCards().size();
            process.getDriver().getProcessCustomerCards().size();
            if( process.getMisc().getAllowedKM() == null || process.getMisc().getAllowedKM().intValue() == 0 ) {
                try {
                    //this.calculateRentalPrices( process, true );
                }
                catch( Exception e ) {
                    log( "ERROR bei Berechnung der Preispositionen für Vorgang: " + process.getId() + " Error: " + e.getMessage() );
                    e.printStackTrace();
                }
            }
        }
        return processes;
    }

    @Override
    public Rental getNextProcess( final Vehicle vehicle, final Date time ) throws CarGuideException
    {
        this.getCurrentSession();
        if( !getHireUser().hasRight( HireRights.VERMITTLER, vehicle ) ) {
            throw new CarGuideException( "Fehlendes Recht Vermittler", 20, 11 );
        }
        if( vehicle == null ) {
            throw new CarGuideException( "Argument vehicle null oder leer", 999, 0 );
        }
        if( time == null ) {
            return rentalDao.searchNextProcess( vehicle, new Date() );
        }
        else {
            return rentalDao.searchNextProcess( vehicle, time );
        }
    }

    @Override
    public Rental getLastProcess( final Vehicle vehicle, final Date time ) throws CarGuideException
    {
        this.getCurrentSession();
        if( !getHireUser().hasRight( HireRights.VERMITTLER, vehicle ) ) {
            throw new CarGuideException( "Fehlendes Recht Vermittler", 20, 11 );
        }
        if( vehicle == null ) {
            throw new CarGuideException( "Argument vehicle null oder leer", 999, 0 );
        }
        if( time == null ) {
            return rentalDao.searchLastProcess( vehicle, new Date() );
        }
        else {
            return rentalDao.searchLastProcess( vehicle, time );
        }
    }

    public List < ProcessVehicleSearchResult > searchVehicleForProcess( final HireProcessSearchParams args ) throws CarGuideException
    {
        this.getCurrentSession();
        List < Vehicle > vehicles = hireVehicleDao.findVehicleByParams( args.getVehicleSearch(), this.getHireUser(), null, null );
        List < ProcessVehicleSearchResult > searchResult = new ArrayList < ProcessVehicleSearchResult >();
        List < ViewPlannerPosition > processes = null;
        if( vehicles != null && vehicles.size() > 0 ) {
            processes = this.searchProcessesForVehicles( vehicles, args.getStartTime(), args.getEndTime(), true );
            List < Vehicle > vehiclesWithUnknownProcesses = new ArrayList < Vehicle >();

            for( Vehicle vehicle : vehicles ) {
                BigDecimal ratePrice = vehicle.getRentalRate() == null ? new BigDecimal( 0 ) : vehicle.getRentalRate().getUnitPrice();
                List < ViewPlannerPosition > vehicleProcesses = new ArrayList < ViewPlannerPosition >();
                if( processes != null ) {
                    for( ViewPlannerPosition process : processes ) {
                        if( process.getOriginVehicleId().equals( vehicle.getId() ) ) {
                            vehicleProcesses.add( process );
                        }
                    }
                }
                if( vehicleProcesses.size() > 1 ) {
                    searchResult
                            .add( new ProcessVehicleSearchResult( vehicle, ProcessVehicleSearchResult.MULTIOCCUPIED, null, null, true ) );
                }
                else if( vehicleProcesses.size() == 1 ) {
                    Integer reason = null;
                    ViewPlannerPosition process = vehicleProcesses.get( 0 );
                    switch( process.getProcessStatus().intValue() ) {
                        case 1:
                            reason = ProcessVehicleSearchResult.RESERVED;
                            break;
                        case 2:
                            reason = ProcessVehicleSearchResult.DRIVING;
                            break;
                        case 3:
                        case 4:
                        default:
                            reason = ProcessVehicleSearchResult.OCCUPIED;
                            log( "Status ist " + process.getProcessStatus() + " fuer Vehicle Id: " + vehicle.getId()
                                    + ". Vorgang falsch bearbeitet bzw. storniert statt gelöscht?" );
                            break;
                    }
                    Date returnDate = process.getReturnTime() == null ? process.getDrivingEndTime() : process.getReturnTime();
                    searchResult.add( new ProcessVehicleSearchResult( vehicle, reason, returnDate, process.getUserQualifiedName(), true ) );
                }
                else if( vehicle.getStatus().isClosed() ) {
                    searchResult.add( new ProcessVehicleSearchResult( vehicle, ProcessVehicleSearchResult.CLOSED, null, null, true ) );
                }
                else {
                    vehiclesWithUnknownProcesses.add( vehicle );
                }
            }
            if( vehiclesWithUnknownProcesses.size() > 0 ) {
                List < ViewPlannerPosition > activeProcesses = this.searchProcessesForVehicles( vehiclesWithUnknownProcesses, null,
                        args.getStartTime(), false );
                for( Vehicle vehicle : vehiclesWithUnknownProcesses ) {
                    String rateName = vehicle.getRentalRate() == null ? "not available" : vehicle.getRentalRate().getSmallQualifiedName();
                    int found = 0;
                    ViewPlannerPosition lastFoundProcess = null;
                    for( ViewPlannerPosition activeProcess : activeProcesses ) {
                        if( vehicle.getId().equals( activeProcess.getOriginVehicleId() ) ) {
                            ++found;
                            lastFoundProcess = activeProcess;
                            break;
                        }
                    }
                    if( found > 1 ) {
                        searchResult.add(
                                new ProcessVehicleSearchResult( vehicle, ProcessVehicleSearchResult.MULTIOVERTIME, null, null, false ) );
                    }
                    else if( found == 1 ) {
                        Date returnDate = lastFoundProcess.getReturnTime() == null ? lastFoundProcess.getDrivingEndTime()
                                : lastFoundProcess.getReturnTime();
                        searchResult.add( new ProcessVehicleSearchResult( vehicle, ProcessVehicleSearchResult.OVERTIME, returnDate,
                                lastFoundProcess.getUserQualifiedName(), false ) );
                    }
                    else {
                        searchResult.add( new ProcessVehicleSearchResult( vehicle, ProcessVehicleSearchResult.FREE, null, null, false ) );
                    }
                }
            }
            Collections.sort( searchResult );
        }
        return searchResult;
    }

    /**
     * Versand von Vorgang Status Mails
     */
    //TODO C: durch Spring eigene Implementation ersetzen
    private boolean sendStatusMail( Rental rental, Integer branchId ) throws CarGuideException
    {
        Map < String, Object > mailParams = new HashMap < String, Object >();
        try {
            List < User > users = hireUserDao.getActiveStatusMailReceiverForBranch( branchId );
            if( users.size() > 0 ) {

                //Betreff Zeile
                String subject = "Vorgang geändert";
                if( rental.getStatus().equals( ProcessStatus.RESERVED.getValue() ) ) {
                    subject = "Neuer Vorgang " + rental.getProcessVehicle().getRegistrationNumber();
                }
                else if( rental.getStatus().equals( ProcessStatus.BACK.getValue() ) ) {
                    subject = "Vorgang zurueckgenommen " + rental.getProcessVehicle().getRegistrationNumber();
                }

                //E-Mail Inhalt
                SimpleDateFormat df = new SimpleDateFormat( "dd.MM.yyyy HH:mm" );
                String registrationNumber = rental.getProcessVehicle().getRegistrationNumber() != null
                        ? rental.getProcessVehicle().getRegistrationNumber()
                        : "";
                StringBuffer mailContent = new StringBuffer();
                mailContent.append( "Hallo,\n\n" );
                mailContent.append( "fuer das Fahrzeug mit Kennzeichen: " + registrationNumber
                        + " wurde ein Vorgang erstellt/verändert. Details nachfolgend:\n\n" );
                mailContent.append( "*Fahrzeug*\n" );
                mailContent.append( "Fahrzeug VIN: " + rental.getProcessVehicle().getVin() + "\n" );
                if( rental.getProcessVehicle().getVehicle().getDmsNo() != null
                        && !rental.getProcessVehicle().getVehicle().getDmsNo().isEmpty() ) {
                    mailContent.append( "Fahrzeug DMS Nr.: " + rental.getProcessVehicle().getVehicle().getDmsNo() + "\n" );
                }
                mailContent
                        .append( "Fahrzeug: " + rental.getProcessVehicle().getBrand() + " " + rental.getProcessVehicle().getType() + "\n" );
                mailContent.append( "*Vertrag*\n" );
                mailContent.append( "Filiale: " + rental.getBranch().getQualifiedName() + "\n" );
                mailContent.append( "Vertrags Nr.: " + rental.getProcessNumber() + "\n" );
                if( rental.getWorkNumber() != null && !rental.getWorkNumber().isEmpty() ) {
                    mailContent.append( "Auftrags Nr.: " + rental.getWorkNumber() + "\n" );
                }
                if( rental.getStatus().intValue() == 1 ) {
                    mailContent.append( "Vorgang Status: Reserviert\n" );
                }
                else if( rental.getStatus().intValue() == 3 ) {
                    mailContent.append( "Vorgang Status: Zurück\n" );
                }
                else {
                    mailContent.append( "Vorgang Status: unbekannt\n" );
                }
                if( rental.getType().equals( Integer.valueOf( 3 ) ) ) {
                    mailContent.append( "Typ: Interne Fahrt\n" );
                }
                else if( rental.getType().equals( Integer.valueOf( 2 ) ) ) {
                    mailContent.append( "Typ: Probefahrt\n" );
                }
                else if( rental.getType().equals( Integer.valueOf( 1 ) ) ) {
                    mailContent.append( "Typ: Vermietung\n" );
                }
                mailContent.append( "*Zeitraum*\n" );
                mailContent.append( "Von: " + df.format( rental.getRentalDate().getStartTime() ) + "\n" );
                mailContent.append( "Bis: " + df.format( rental.getRentalDate().getEndTime() ) + "\n" );
                mailContent.append(
                        "\nVorgang bearbeiten(Bei aktiven Login): http://carguide.compact.de:8080/HireGui/planner/main.zul?process="
                                + rental.getId() + "\n" );

                //E-Mail Parameter
                mailParams.put( "host", "mail3.caix.net" );
                mailParams.put( "port", "465" );
                mailParams.put( "startttls", "true" );
                mailParams.put( "ssl", true );
                mailParams.put( "smtpAuth", true );
                //            mailParams.put( "host", "pbox2.compact.intranet" );
                //            mailParams.put( "port", "25" );
                mailParams.put( "sender", "hire@compact.de" );
                mailParams.put( "mailSubject", "[HIRE] " + subject );
                List < String > userMails = new ArrayList < String >();
                for( User user : users ) {
                    if( !user.getId().equals( getHireUser().getUserId() ) ) {
                        if( user.getContact().getMail() != null ) {
                            userMails.add( user.getContact().getMail() );
                        }
                        else {
                            log( "User: " + user.getId() + " has no Mail Address for Company Branch: " + branchId );
                        }
                    }
                }
                if( userMails.size() < 1 ) {
                    return true;
                }
                mailParams.put( "receiver", userMails );
                mailContent.append(
                        "\nDies ist eine automatisch generierte E-Mail aus CarGuide HIRE. Wenn Sie keine weiteren E-Mails zu Vorgaengen erhalten wollen, koennen Sie die Status Mail in Ihren Benutzereinstellungen abschalten.\n" );
                mailContent.append(
                        "\n--\nCompAct GmbH\nComputer und Kommunikation\n\nStralsunder Str. 62\n33605 Bielefeld\nGERMANY\n\nFon: +49 521 29982-22\nFax: +49 521 2603198\n" );
                mailContent.append( "Eingetragen im Amtsgericht Bielefeld, HRB 35337\nGeschaeftsfuehrer: Roman Stahl" );
                mailParams.put( "mailContent", mailContent.toString() );
                //log( "Mail Parameter: " + mailParams.toString() );
                //new SendEmail( mailParams );
                return true;
            }
        }
        catch( Exception e ) {
            log( "Fehler bei der E-Mail Versendung: " + e.getMessage() );
            log( "Mail Parameter: " + mailParams.toString() );
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Set customer no for new processCustomer
     * 
     * @param processCustomer
     * @param process
     * @return false, if
     * @throws CarGuideProcessException
     */
    private boolean setCustomerNoForProcessCustomer( ProcessCustomer processCustomer, Rental process ) throws CarGuideProcessException
    {

        if( process.getCompany().isHireDebitor() ) {
            /// TODO A: use accountingDao.hasAccountingForDebitors( process.getBranch().getId() ) instead
            if( processCustomer.getCustomerNo() == null || processCustomer.getCustomerNo().trim().equals( "" ) ) {
                //TODO B: check if ProcessCustomer without customer number as customer exist
                //TODO A: create flag if customers visible for all branches
                //TODO A: ask with data protection commissioner about this "feature"
                //TODO A: if customer comes from other system, clarify problems 
                String debitorNo = accountingDao.provideNewDebitorNumberForBranchId( process.getBranch().getId() );
                processCustomer.setCustomerNo( debitorNo );
                Customer newCustomer = processCustomer.createCustomerFromProcessCustomer();
                newCustomer.setCompany( process.getCompany() );
                newCustomer.setBranch( process.getBranch() );
                Customer customer = customerDao.saveCustomer( newCustomer );
                if( customer == null ) {
                    throw new CarGuideProcessException( "Fehler beim Speichern des Kunden", 9, 48, true );
                }
                processCustomer.setCustomer( customer );
                return true;
            }
        }
        return false;
    }

    @Override
    public Rental refreshCalculatedPeriods( Rental process ) throws CarGuideException
    {
        this.getCurrentSession();
        //        if( !getHireUser().isCompact() ) {
        //            throw new CarGuideException( "Nicht genügend Rechte" );
        //        }

        try {
            Set < TransferProcessPeriod > transferPeriods = ProcessPeriod.convertToTransferProcessPeriod( process.getProcessPeriods() );
            ProcessPeriodCalculator periodClaculator = new ProcessPeriodCalculator( ProcessStatus.getStatusFromInt( process.getStatus() ),
                    process.getRentalDate().getStartTime(), process.getRentalDate().getCurrentReturnTime(), process.getRate().getUnit(),
                    process.getRate().getPeriodStartDate(), transferPeriods );

            transferPeriods = periodClaculator.refreshCalculatedPeriods();

            process.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, process ) );
        }
        catch( Exception e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return process;
    }

    @Override
    public Rental updateExistingAccountingPeriodsForProcess( Rental process ) throws Exception
    {
        try {
            this.getCurrentSession();
            Set < TransferProcessPeriod > transferPeriods = ProcessPeriod.convertToTransferProcessPeriod( process.getProcessPeriods() );
            ProcessPeriodCalculator periodClaculator = new ProcessPeriodCalculator( ProcessStatus.getStatusFromInt( process.getStatus() ),
                    process.getRentalDate().getStartTime(), process.getRentalDate().getCurrentReturnTime(), process.getRate().getUnit(),
                    process.getRate().getPeriodStartDate(), transferPeriods );

            transferPeriods = periodClaculator.refreshCalculatedPeriods();

            process.setProcessPeriods( ProcessPeriod.convertToTransferProcessPeriod( transferPeriods, process ) );
        }
        catch( Exception e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return process;
    }

}
